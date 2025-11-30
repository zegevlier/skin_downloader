use base64::prelude::*;
use itertools::Itertools;
use rayon::prelude::*;
use ruzstd::StreamingDecoder;

use std::{io::BufRead, sync::Arc};

mod types;
use types::*;

const TASK_LIMIT: usize = 1000;

const STRING_PREFIX: &str = "http://textures.minecraft.net/texture/";

fn get_db() -> sled::Db {
    sled::open("skins.sled").unwrap()
}

async fn run_downloader(db: sled::Db) {
    let source_file = std::fs::File::open("mojang.jsonl.zst").unwrap();
    let source = std::io::BufReader::new(StreamingDecoder::new(source_file).unwrap());

    let skin_id_iterator = source.lines().filter_map(|line| {
        let line = line.unwrap();

        let mojang_response: MojangResponse = serde_json::from_str(&line).unwrap();

        let base64_decoded = base64::prelude::BASE64_STANDARD
            .decode(mojang_response.properties[0].value.as_bytes())
            .unwrap();
        let base64_decoded_str = String::from_utf8(base64_decoded).unwrap();

        let textures: Textures = serde_json::from_str(&base64_decoded_str).unwrap();

        if let Some(skin) = textures.textures.skin {
            let skin_id = skin.url.rsplit('/').next().unwrap();
            Some(skin_id.to_owned())
        } else {
            None
        }
    });

    let semaphore = Arc::new(tokio::sync::Semaphore::new(TASK_LIMIT));

    tokio::task::spawn({
        let mut prev_len = db.len();
        let db = db.clone();
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        async move {
            loop {
                interval.tick().await;
                println!("Downloaded {} skins in the last 30 seconds (~{} skins/sec)", db.len() - prev_len, (db.len() - prev_len) / 30);
                prev_len = db.len();
            }
        }
    });

    let connection = reqwest::Client::new();

    for skin_id in skin_id_iterator {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let db = db.clone();
        let connection = connection.clone();
        tokio::spawn(async move {
            if db.contains_key(skin_id.as_bytes()).unwrap() {
                drop(permit);
                return;
            }

            let skin_url = format!("{}{}", STRING_PREFIX, skin_id);
            let response = connection.get(&skin_url).send().await.unwrap();
            if !response.status().is_success() {
                println!("Failed to download skin {}", skin_id);
                drop(permit);
                return;
            }
            let skin = response.bytes().await.unwrap();
            let skin = skin.to_vec();

            db.insert(skin_id, skin).unwrap();

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            drop(permit);
        });
    }

    // We wait until all tasks are done
    drop(semaphore.acquire_many(TASK_LIMIT as u32).await.unwrap());

    println!("All tasks are done!");
}

fn validate_and_print_db() {
    let db = get_db();

    for entry in db.iter() {
        let (key, value) = entry.unwrap();
        let key = std::str::from_utf8(&key).unwrap();
        let value = value.to_vec();

        let cursor = std::io::Cursor::new(value);

        match image::load(cursor, image::ImageFormat::Png) {
            Ok(_) => {}
            Err(_) => {
                println!("Skin {} is invalid", key);
            }
        }
    }

    println!("There are {} skins in the database", db.len());
}

fn export() {
    let source_db = get_db();

    let mut target_db = rusqlite::Connection::open("skins.sqlite").unwrap();

    target_db
        .execute(
            "CREATE TABLE IF NOT EXISTS skins (id TEXT PRIMARY KEY, skin BLOB)",
            [],
        )
        .unwrap();

    println!("Exporting skins");
    let mut total = 0;
    let batch_size = 2500;
    for entry in &source_db.iter().chunks(batch_size) {
        let transaction = target_db.transaction().unwrap();

        let mut statement = transaction
            .prepare("INSERT INTO skins (id, skin) VALUES (?, ?)")
            .unwrap();

        let entry = entry.into_iter().collect::<Vec<_>>();
        let entry = entry
            .into_par_iter()
            .map(|e| {
                let (key, value) = e.unwrap();
                let key = std::str::from_utf8(&key).unwrap().to_string();
                let value = value.to_vec();
                let optimized_image =
                    oxipng::optimize_from_memory(&value, &oxipng::Options::from_preset(2)).unwrap();
                (key, optimized_image)
            })
            .collect::<Vec<_>>();

        for (key, optimized_image) in entry {
            match statement.execute((&key, optimized_image)) {
                Ok(_) => {}
                Err(e) => {
                    if e.to_string().contains("UNIQUE constraint failed") {
                        continue;
                    }
                    println!("Failed to insert skin {} {}", key, e);
                }
            }
        }
        drop(statement);
        transaction.commit().unwrap();
        total += 1;
        println!("Exported {} skins", total * batch_size);
    }

    println!("Export done");
}

async fn live_export(source_db: sled::Db) {
    let target_db = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open("skins.sqlite").unwrap(),
    ));

    // Create the table
    {
        let db = target_db.lock().unwrap();
        db.execute(
            "CREATE TABLE IF NOT EXISTS skins (id TEXT PRIMARY KEY, skin BLOB)",
            [],
        )
        .unwrap();
    }

    let mut processed_keys = std::collections::HashSet::new();
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

    println!("Starting live export and optimization...");

    loop {
        interval.tick().await;

        // Collect new keys that haven't been processed yet
        let new_entries: Vec<_> = source_db
            .iter()
            .filter_map(|entry| {
                let (key, value) = entry.ok()?;
                let key_str = std::str::from_utf8(&key).ok()?.to_string();
                if !processed_keys.contains(&key_str) {
                    Some((key_str, value.to_vec()))
                } else {
                    None
                }
            })
            .collect();

        if new_entries.is_empty() {
            continue;
        }

        println!(
            "Processing {} new skins for optimization and export",
            new_entries.len()
        );

        // Process entries in parallel using rayon
        let optimized_entries: Vec<_> = new_entries
            .into_par_iter()
            .filter_map(|(key, value)| {
                match oxipng::optimize_from_memory(&value, &oxipng::Options::from_preset(2)) {
                    Ok(optimized) => Some((key, optimized)),
                    Err(e) => {
                        println!("Failed to optimize skin {}: {}, using original", key, e);
                        Some((key, value)) // Fall back to original
                    }
                }
            })
            .collect();

        // Insert optimized entries into SQLite in batches
        if !optimized_entries.is_empty() {
            let db = target_db.clone();
            let batch_size = 1000;

            for chunk in optimized_entries.chunks(batch_size) {
                let mut db = db.lock().unwrap();
                let transaction = db.transaction().unwrap();

                let mut statement = transaction
                    .prepare("INSERT OR IGNORE INTO skins (id, skin) VALUES (?, ?)")
                    .unwrap();

                for (key, optimized_data) in chunk {
                    if let Err(e) = statement.execute((key, optimized_data)) {
                        println!("Failed to insert skin {}: {}", key, e);
                    } else {
                        processed_keys.insert(key.clone());
                    }
                }

                drop(statement);
                transaction.commit().unwrap();
            }

            println!(
                "Exported {} optimized skins. Total processed: {}",
                optimized_entries.len(),
                processed_keys.len()
            );
        }
    }
}

#[tokio::main]
async fn main() {
    let option = match std::env::args().nth(1) {
        Some(option) => option,
        None => {
            println!("Usage: skin_downloader <option>");
            println!("Options:");
            println!("  download              - Download skins to sled database");
            println!("  download-and-export   - Download skins while simultaneously optimizing and exporting to SQLite");
            println!("  validate              - Validate skins in the database");
            println!("  export                - Export skins from sled to SQLite");
            panic!("No option provided");
        }
    };
    if option == "download" {
        run_downloader(get_db()).await;
    } else if option == "download-and-export" {
        let db = get_db();

        // Start the live export task
        let export_handle = tokio::spawn(live_export(db.clone()));

        // Start downloading
        run_downloader(db).await;

        // Keep the export running for a bit longer to catch any final skins
        println!("Download complete, waiting for final optimizations...");
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        export_handle.abort();
    } else if option == "validate" {
        validate_and_print_db();
    } else if option == "export" {
        export();
    } else {
        panic!("Invalid option");
    }
}
