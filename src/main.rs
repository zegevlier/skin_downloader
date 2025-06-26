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

    // Create SQLite database and table (creates file if it doesn't exist)
    println!("Creating/opening SQLite database: skins.sqlite");
    let sqlite_db = Arc::new(tokio::sync::Mutex::new(
        rusqlite::Connection::open("skins.sqlite")
            .expect("Failed to create/open SQLite database file")
    ));
    
    {
        let db = sqlite_db.lock().await;
        db.execute(
            "CREATE TABLE IF NOT EXISTS skins (id TEXT PRIMARY KEY, skin BLOB)",
            [],
        )
        .expect("Failed to create skins table");
        // Enable WAL mode for better concurrent performance
        db.execute("PRAGMA journal_mode=WAL", []).unwrap();
        // Increase cache size for better performance
        db.execute("PRAGMA cache_size=10000", []).unwrap();
        println!("Database initialized successfully");
    }

    // Create a channel for batching database operations
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, Vec<u8>)>(1000);
    
    // Spawn a dedicated database writer task
    let db_writer = {
        let sqlite_db = sqlite_db.clone();
        tokio::spawn(async move {
            let mut batch = Vec::new();
            const BATCH_SIZE: usize = 50;
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
            
            loop {
                tokio::select! {
                    // Receive new items to add to batch
                    Some((skin_id, optimized_image)) = rx.recv() => {
                        batch.push((skin_id, optimized_image));
                        
                        // Process batch when it's full
                        if batch.len() >= BATCH_SIZE {
                            process_batch(&sqlite_db, &mut batch).await;
                        }
                    }
                    // Process remaining items on timer
                    _ = interval.tick() => {
                        if !batch.is_empty() {
                            process_batch(&sqlite_db, &mut batch).await;
                        }
                    }
                    // Channel closed, process remaining and exit
                    else => {
                        if !batch.is_empty() {
                            process_batch(&sqlite_db, &mut batch).await;
                        }
                        break;
                    }
                }
            }
        })
    };

    // Pre-load existing skin IDs into a HashSet for fast lookups
    let existing_skins = {
        let db = sqlite_db.lock().await;
        let mut stmt = db.prepare("SELECT id FROM skins").unwrap();
        let rows = stmt.query_map([], |row| {
            row.get::<_, String>(0)
        }).unwrap();
        
        let mut set = std::collections::HashSet::new();
        for row in rows {
            set.insert(row.unwrap());
        }
        println!("Loaded {} existing skins into memory", set.len());
        Arc::new(tokio::sync::RwLock::new(set))
    };

    // Download tasks - similar to original but with batched database writes
    for skin_id in skin_id_iterator {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let connection = connection.clone();
        let tx = tx.clone();
        let existing_skins = existing_skins.clone();
        
        tokio::spawn(async move {
            // Quick check if skin already exists (no database lock needed)
            {
                let existing = existing_skins.read().await;
                if existing.contains(&skin_id) {
                    drop(permit);
                    return;
                }
            }

            let skin_url = format!("{}{}", STRING_PREFIX, skin_id);
            let response = match connection.get(&skin_url).send().await {
                Ok(resp) => resp,
                Err(_) => {
                    drop(permit);
                    return;
                }
            };
            
            if !response.status().is_success() {
                drop(permit);
                return;
            }
            
            let skin_bytes = match response.bytes().await {
                Ok(bytes) => bytes,
                Err(_) => {
                    drop(permit);
                    return;
                }
            };
            
            let skin_vec = skin_bytes.to_vec();

            // Optimize the image before storing
            let optimized_image = match oxipng::optimize_from_memory(&skin_vec, &oxipng::Options::from_preset(2)) {
                Ok(img) => img,
                Err(_) => {
                    drop(permit);
                    return;
                }
            };

            // Add to existing skins set and send to database writer
            {
                let mut existing = existing_skins.write().await;
                existing.insert(skin_id.clone());
            }
            
            // Send to database writer (non-blocking)
            if tx.send((skin_id, optimized_image)).await.is_err() {
                // Channel closed
            }

            // Much shorter delay - similar to original
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            drop(permit);
        });
    }

    // Close the channel to signal database writer to finish
    drop(tx);
    
    // Wait for all download tasks to complete
    drop(semaphore.acquire_many(TASK_LIMIT as u32).await.unwrap());
    
    // Wait for database writer to finish
    db_writer.await.unwrap();

    // Get final count
    let db = sqlite_db.lock().await;
    let mut stmt = db.prepare("SELECT COUNT(*) FROM skins").unwrap();
    let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
    
    println!("Direct download complete! Total skins in database: {}", count);
}

async fn process_batch(
    sqlite_db: &Arc<tokio::sync::Mutex<rusqlite::Connection>>, 
    batch: &mut Vec<(String, Vec<u8>)>
) {
    if batch.is_empty() {
        return;
    }
    
    let mut db = sqlite_db.lock().await;
    let transaction = db.transaction().unwrap();
    let mut stmt = transaction.prepare("INSERT OR IGNORE INTO skins (id, skin) VALUES (?, ?)").unwrap();
    
    for (skin_id, optimized_image) in batch.drain(..) {
        if let Err(e) = stmt.execute((&skin_id, &optimized_image)) {
            eprintln!("Failed to insert skin {}: {}", skin_id, e);
        }
    }
    
    drop(stmt);
    if let Err(e) = transaction.commit() {
        eprintln!("Failed to commit transaction: {}", e);
    }
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
