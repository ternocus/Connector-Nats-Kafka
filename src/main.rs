#![allow(non_snake_case)]
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::{io::{self, Write}};
use std::thread;
use std::time::Duration;
use kafka::producer::{Producer, Record};
use kafka::consumer::Consumer;
use nats;

const CONFIGURATION_FILE:&str = "/tmp/Config.txt";
const NUM_THREAD:i32 = 1;
const DEBUG:bool = true;

fn main() {
    println!("\n\n----------------------------------------------------");
    println!("Starting connector for Nats and Kafka servers...");
    if DEBUG {
        println!("Debug modality active");
    }
    println!("----------------------------------------------------\n");

    let file = std::fs::File::create(CONFIGURATION_FILE).expect("create failed");

    println!("-- Nats configuration --");
    let file:std::fs::File = getNatsServerUrl(file);

    println!("\n-- Kafka configuration --");
    let file:std::fs::File = getKafkaServerUrl(file);

    println!("\n-- Subject configuration --\n");
    getSubject(file);

    println!("\n-- Starting threads... --\n");

    let mut thread:std::vec::Vec<std::thread::JoinHandle<()>> = Vec::new();
    thread.push(std::thread::spawn(|| {
        natsKafkaConnection();
    }));
    thread::sleep(Duration::from_millis(1));
    thread.push(std::thread::spawn(|| {
       kafkaNatsConnection();
    }));

    for x in 1..NUM_THREAD {
        thread.push(std::thread::spawn(|| {
            natsKafkaConnection();
        }));
        thread::sleep(Duration::from_millis(1));
        thread.push(std::thread::spawn(|| {
           kafkaNatsConnection();
        }));
    }

    println!("----------------------------------------------------");
    println!("Connector started");
    println!("----------------------------------------------------\n");

    for y in 0..(NUM_THREAD*2) as usize{
        let thr = thread.remove(y);
        thr.join().unwrap();
    }

    println!("----------------------------------------------------");
    println!("Connector stopped");
    println!("----------------------------------------------------\n");
}

fn getNatsServerUrl(mut file:std::fs::File) -> std::fs::File {
    let mut nats_server_url = String::new();
    print!("\nPort to connect to Nats server (nats://localhost:4222): ");
    io::stdout().flush().unwrap();
    io::stdin().read_line(&mut nats_server_url).expect("Error: can't read it.");

    if nats_server_url == "\n" {
        file.write_all("nats://localhost:4222".as_bytes()).expect("Failed to compile the Configuration file");
    } else {
        nats_server_url.remove(nats_server_url.len() - 1);
    }
    file.write_all(nats_server_url.as_bytes()).expect("Failed to compile the Configuration file");
    return file;
}

fn getKafkaServerUrl(mut file:std::fs::File) -> std::fs::File {
    let mut kafka_server_url = String::new();
    print!("\nPort to connect to Kafka server (localhost:9092):");
    io::stdout().flush().unwrap();
    io::stdin().read_line(&mut kafka_server_url).expect("Error: can't read it.");

    if kafka_server_url == "\n" {
        file.write_all("localhost:9092".as_bytes()).expect("Failed to compile the Configuration file");
    } else {
        kafka_server_url.remove(kafka_server_url.len() - 1);
    }
    file.write_all(kafka_server_url.as_bytes()).expect("Failed to compile the Configuration file");
    return file;
}

fn getSubject(mut file:std::fs::File) {
    let mut subject = String::new();
    loop {
        print!("Enter the subject for sending and receiving messages with Nats and kafka server: ");
        io::stdout().flush().unwrap();
        io::stdin().read_line(&mut subject).expect("Error: can't read it.");

        subject.remove(subject.len() - 1);
        if subject.is_empty() {
            println!("Error: You must enter a valid topic, retry.");
        } else {
            break;
        }
    }
    file.write_all(subject.as_bytes()).expect("Failed to compile the Configuration file");
}

fn readConfigurationFile() -> Vec<String> {
    let mut result = Vec::new();

    let file = File::open(CONFIGURATION_FILE).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        result.push(line.unwrap());
    }
    return result;
}

fn natsKafkaConnection() {
    let config:Vec<String> = readConfigurationFile();
    
    let mut prod: kafka::producer::Producer = match Producer::from_hosts(vec![config[1].clone()]).with_client_id("Thread 1".to_owned()).create() {
        Ok(v) => {
            if DEBUG {
                println!("Connect to sever kafka (producer).");
            }
            v
        }
        Err(err) => {
            if DEBUG {
                println!("Error to connect to kafka server (producer): {}", err);
            }
            return
        }
    };

    let nc: nats::Connection = match nats::Options::new().with_name("NatsKafka").connect(&config[0].clone()) {
        Ok(v) =>  {
            if DEBUG {
                println!("NatsKafka thread: Connect to sever Nats (consumer).");
            }
            v
        }
        Err(err) => {
            if DEBUG {
                println!("NatsKafka thread: error to connect with Nats server: {}", err);
            }
            return;
        }
    };

    match nc.subscribe(&config[2]) {
        Ok(v) => {
            loop {
                for msg in v.messages() {
                    match msg.reply {
                        Some(x) => {
                            if !x.eq("KafkaNats") {
                                if DEBUG {
                                    println!("NatsKafka thread: Recived from Nats server: {}, data: {:?}", msg.subject, String::from_utf8_lossy(&msg.data));
                                }
                                prod.send(&Record {
                                    topic: &msg.subject,
                                    partition: -1,
                                    key: "NatsKafka",
                                    value: String::from_utf8_lossy(&msg.data).as_bytes(),
                                }).expect("Failed to send message to kafka server");
                            }
                        },
                        None => {
                                prod.send(&Record {
                                    topic: &msg.subject,
                                    partition: -1,
                                    key: "NatsKafka",
                                    value: String::from_utf8_lossy(&msg.data).as_bytes(),
                                }).expect("Failed to send message to kafka server");
                         }
                    }
                }
            }
        },
        Err(err) => {
            if DEBUG {
                println!("Error to subscribe subject in Nats server: {}", err);
            }
        }
    };
}

fn kafkaNatsConnection() {
    let config:Vec<String> = readConfigurationFile(); 
    
    let mut cons: kafka::consumer::Consumer = match Consumer::from_hosts(vec![config[1].clone()]).with_client_id("Thread 2".to_owned()).with_topic(config[2].clone()).create() {
        Ok(v) => {
            if DEBUG { 
                println!("Connect to sever kafka (consumer).");
            }
            v
        }
        Err(err) => {
            if DEBUG {
                println!("Error to connect to kafka server (consumer): {}",err);
            }
            return
        }
    };

    let nc: nats::Connection = match nats::Options::new().with_name("KafkaNats").connect(&config[0].clone()) {
        Ok(v) =>  {
            if DEBUG {
                println!("KafkaNats thread: Connect to sever Nats (producer).");
            }
            v
        }
        Err(err) => {
            if DEBUG {
                println!("KafkaNats thread: error to connect with Nats server: {}", err);
            }
            return;
        }
    };

    loop {
        match cons.poll() {
            Ok(ms) =>{
                for message in ms.iter() {
                    for m in message.messages() {
                        if !String::from_utf8_lossy(m.key).eq("NatsKafka") {
                            if DEBUG {
                                println!("KafkaNats thread: Recived from kafka server: {:?}, {}, data: {:?}", String::from_utf8_lossy(m.key), message.topic(), String::from_utf8_lossy(m.value));
                            }
                            nc.publish_with_reply_or_headers(message.topic(), Some("KafkaNats"), None, m.value).expect("Failed to send message to Nats server");
                        }
                    }
                    let _ = cons.consume_messageset(message);
                }
                let _ = cons.commit_consumed();
            }
            Err(err) => {
                if DEBUG {
                    println!("Errore to recive messages: {}", err);
                }
                break;
            }
        };
    }
}
