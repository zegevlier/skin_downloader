use base64::prelude::*;
use itertools::Itertools;
use ruzstd::StreamingDecoder;

use std::{io::BufRead, sync::Arc};

mod types;
use types::*;

const TASK_LIMIT: usize = 1000;

const STRING_PREFIX: &str = "http://textures.minecraft.net/texture/";

fn get_db() -> sled::Db {
    sled::open("skins.sled").unwrap()
}

async fn run_downloader() {
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

    let db = get_db();

    // tokio::task::spawn({
    //     let db = db.clone();
    //     let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(600));
    //     async move {
    //         loop {
    //             interval.tick().await;
    //             println!("Database has {} skins", db.len());
    //         }
    //     }
    // });

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

        for entry in entry {
            let (key, value) = entry.unwrap();
            let key = std::str::from_utf8(&key).unwrap().to_string();
            let value = value.to_vec();
            let optimized_image =
                oxipng::optimize_from_memory(&value, &oxipng::Options::from_preset(2)).unwrap();

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

async fn run_downloader_direct() {
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
    let connection = reqwest::Client::new();

    // Create SQLite database and table
    let sqlite_db = Arc::new(tokio::sync::Mutex::new(
        rusqlite::Connection::open("skins.sqlite").unwrap()
    ));
    
    {
        let db = sqlite_db.lock().await;
        db.execute(
            "CREATE TABLE IF NOT EXISTS skins (id TEXT PRIMARY KEY, skin BLOB)",
            [],
        )
        .unwrap();
    }

    let mut processed_count = 0;
    let batch_size = 100;
    let mut batch_handles = Vec::new();

    for skin_id in skin_id_iterator {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let connection = connection.clone();
        let sqlite_db = sqlite_db.clone();
        
        let handle = tokio::spawn(async move {
            // Check if skin already exists in SQLite
            {
                let db = sqlite_db.lock().await;
                let mut stmt = db.prepare("SELECT 1 FROM skins WHERE id = ?").unwrap();
                if stmt.exists([&skin_id]).unwrap() {
                    drop(permit);
                    return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
                }
            }

            let skin_url = format!("{}{}", STRING_PREFIX, skin_id);
            let response = connection.get(&skin_url).send().await?;
            
            if !response.status().is_success() {
                println!("Failed to download skin {}", skin_id);
                drop(permit);
                return Ok(());
            }
            
            let skin_bytes = response.bytes().await?;
            let skin_vec = skin_bytes.to_vec();

            // Optimize the image before storing
            let optimized_image = oxipng::optimize_from_memory(&skin_vec, &oxipng::Options::from_preset(2))?;

            // Insert directly into SQLite
            {
                let db = sqlite_db.lock().await;
                let mut stmt = db.prepare("INSERT OR IGNORE INTO skins (id, skin) VALUES (?, ?)")?;
                stmt.execute((&skin_id, &optimized_image))?;
            }

            // Add a small delay to be respectful to the server
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            drop(permit);
            Ok(())
        });

        batch_handles.push(handle);

        // Process in batches to avoid overwhelming memory
        if batch_handles.len() >= batch_size {
            for handle in batch_handles.drain(..) {
                if let Err(e) = handle.await {
                    println!("Task failed: {}", e);
                }
            }
            processed_count += batch_size;
            println!("Processed {} skins", processed_count);
        }
    }

    // Process remaining handles
    for handle in batch_handles {
        if let Err(e) = handle.await {
            println!("Task failed: {}", e);
        }
    }

    // Wait until all tasks are done
    drop(semaphore.acquire_many(TASK_LIMIT as u32).await.unwrap());

    // Get final count
    let db = sqlite_db.lock().await;
    let mut stmt = db.prepare("SELECT COUNT(*) FROM skins").unwrap();
    let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
    
    println!("Direct download complete! Total skins in database: {}", count);
}

#[tokio::main]
async fn main() {
    let option = match std::env::args().nth(1) {
        Some(option) => option,
        None => {
            println!("Usage: skin_downloader <option>");
            println!("Options:");
            println!("  download       - Download skins to sled database");
            println!("  download-direct - Download skins directly to SQLite database");
            println!("  validate       - Validate skins in the database");
            println!("  export         - Export skins from sled to SQLite");
            panic!("No option provided");
        }
    };
    if option == "download" {
        run_downloader().await;
    } else if option == "download-direct" {
        run_downloader_direct().await;
    } else if option == "validate" {
        validate_and_print_db();
    } else if option == "export" {
        export();
    } else {
        panic!("Invalid option");
    }
}
