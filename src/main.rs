extern crate serde_yaml;
extern crate chrono;

use std::collections::HashMap;
// use std::fs::File;
use std::io;
use std::io::prelude::*;     
use std::net::UdpSocket;
use std::str::from_utf8;
use std::net::{TcpListener, TcpStream};

use chrono::prelude::*;

fn load_config() -> io::Result<String> {
    /*
    let mut f = File::open("./config.yml")?;
    let mut contents = String::new();
    f.read_to_string(&mut contents)?;
    Ok(contents)
    */

    let contents = String::from("
            input:
              stdin
            output:
              stdout"
  );
  Ok(contents)
}

fn format_log<'a>(message: String, host: String) -> HashMap<&'a str, String> {
    let mut log_record = HashMap::new();
    log_record.insert("timestamp", Utc::now().to_rfc3339());
    log_record.insert("message", message);
    log_record.insert("host", host);
    log_record.insert("_flut_version", String::from("1"));
    log_record.insert("_flut_processed", String::from("true"));
    return log_record
}


fn handle_client(stream: &mut TcpStream, output: String) {

    let host = format!("{:?}", stream.peer_addr().unwrap());

    loop {
        let mut message = String::new();
        let _ = stream.read_to_string(&mut message).unwrap_or(0);

        println!("{}", message);
        
        let log = format_log(message, host.clone());

        if output == "stdout" {
            println!("{:?}", log);
        } else {
            panic!("unsupported output");
        } 
    }

    
}

fn main() {
    let yaml_str = load_config().expect("config not there");

    let decoded: HashMap<String, String> = serde_yaml::from_str(&yaml_str).expect("yaml malformed");

    println!("{:?}", decoded);

    let output = decoded.get("output").expect("malformed config, missing output");
    let input = decoded.get("input").expect("malformed config, missing input");

    match input.as_ref() {
        "stdin" => {
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                let log = format_log(line.expect("could not get line"), String::from("stdin://localhost"));                
                
                if output == "stdout" {
                    println!("{:?}", log);
                } else {
                    panic!("unsupported output");
                }
            }
        },
        "udp" => {
            let port = 3000;
            let mut socket = UdpSocket::bind(format!("127.0.0.1:{}", port)).expect("udp socket could not get created");
            
            loop {       
                let mut buf = [0; 512];         
                let (amt, src) = socket.recv_from(&mut buf).expect("error in reading udp datagrams");

                let message = from_utf8(&buf[0..amt]).expect("malformed datagram");
                let log = format_log(String::from(message), format!("{:?}", src));

                if output == "stdout" {
                    println!("{:?}", log);
                } else {
                    panic!("unsupported output");
                }              
            }

        },
        "tcp" => {
            let port = 3000;
            let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).expect("unable to establish tcp socket");
            println!("Listening on 3001 tcp");
            for stream in listener.incoming() {
                println!("received a connection");
                handle_client(&mut stream.expect("unable to unwrap tcp stream"), output.clone());
            }
        }
        _ => {
            panic!("unsupported input type")
        }
    }



}
