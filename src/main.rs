#![allow(non_snake_case)]
use std::{io::{self, Write}};
use kafka::producer::{Producer, Record};
use kafka::consumer::Consumer;
use nats;

const DEBUG:bool = false;

fn main() {
    println!("\n\n----------------------------------------------------");
    println!("Starting connector for Nats and Kafka servers...");
    if DEBUG {
        println!("Debug modality active");
    }
    println!("----------------------------------------------------\n");

    println!("-- Nats configuration --");
    let nats_server_url:String = getNatsServerUrl();
    let nats_server_url_copy:String = nats_server_url.clone();

    println!("\n-- Kafka configuration --");
    let kafka_server_url:String = getKafkaServerUrl();

    println!("\n-- Subject configuration --\n");
    let nats_subject:String = getSubject();
    let kafka_subject:String = nats_subject.clone();

    println!("\n-- Connector starting... --\n");
    let producer: kafka::producer::Producer = match Producer::from_hosts(vec![kafka_server_url.clone()]).with_client_id("Thread 1".to_owned()).create() {
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

    let consumer: kafka::consumer::Consumer = match Consumer::from_hosts(vec![kafka_server_url]).with_client_id("Thread 2".to_owned()).with_topic(kafka_subject).create() {
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

    let natsKafkaThread =std::thread::spawn(move || {
        natsKafkaConnection(nats_server_url, producer, nats_subject);
    });
    let kafkaNatsThread = std::thread::spawn(move || {
       kafkaNatsConnection(nats_server_url_copy, consumer);
    });

    let _ = natsKafkaThread.join();
    let _ = kafkaNatsThread.join();

    println!("\n\n----------------------------------------------------");
    println!("Shutdown connector");
    println!("----------------------------------------------------\n");
}

fn getNatsServerUrl() -> String {
    let mut nats_server_url = String::new();
    print!("\nPort to connect to Nats server (nats://localhost:4222): ");
    io::stdout().flush().unwrap();
    io::stdin().read_line(&mut nats_server_url).expect("Error: can't read it.");

    if nats_server_url == "\n" {
        nats_server_url = "nats://localhost:4222".to_string();
    } else {
        nats_server_url.remove(nats_server_url.len() - 1);
    }
    return nats_server_url;
}

fn getKafkaServerUrl() -> String {
    let mut kafka_server_url = String::new();
    print!("\nPort to connect to Kafka server (localhost:9092):");
    io::stdout().flush().unwrap();
    io::stdin().read_line(&mut kafka_server_url).expect("Error: can't read it.");

    if kafka_server_url == "\n" {
        return "localhost:9092".to_string();
    } else {
        kafka_server_url.remove(kafka_server_url.len() - 1);
    }
    return kafka_server_url;
}

fn getSubject() -> String {
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
    return subject;
}

fn natsKafkaConnection(nats_server_url:String, producer:kafka::producer::Producer, nats_subject:String) {
    let mut prod = producer;
    let nc: nats::Connection = match nats::Options::new().with_name("NatsKafka").connect(&nats_server_url) {
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

    match nc.subscribe(&nats_subject) {
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

fn kafkaNatsConnection(nats_server_url:String, consumer:kafka::consumer::Consumer) {
    let mut cons = consumer;
    let nc: nats::Connection = match nats::Options::new().with_name("KafkaNats").connect(&nats_server_url) {
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
