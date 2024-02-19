use std::thread;
use core::{time};


pub fn put_http(body: String, conn_string: &str){
    let client = reqwest::blocking::Client::new();
    let _ = client.put(conn_string)
        // .form(&params)
        .body(body)
        .send();
}

pub fn get_http(conn_string: &str, sleep_time_secs: u64, timeout_secs: u64) -> Option<String>{
    let mut timeout_timer: u64 = 0;
    loop {
        let response = reqwest::blocking::get(conn_string).unwrap();
        if response.status()==200{
            return Some(response.text().unwrap());
        }
        if timeout_timer>=timeout_secs{return None}
        thread::sleep(time::Duration::from_secs(sleep_time_secs));
        timeout_timer+=sleep_time_secs;
    };
}
