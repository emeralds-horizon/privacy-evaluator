mod structs;
mod privacy;
mod io_http;
use core::time;
use kdam::tqdm;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Instant;
use structs::{Coordinate, Pois, Record, TrajCollection, Trajectory, TrajDataFrame};
use privacy::{LocationAttack, HomeWorkAttack, LocationTimeAttack, UniqueLocationAttack, LocationSequenceAttack};
use io_http::{put_http, get_http};
use std::thread;
use polars::{prelude::*, lazy::dsl::col};
use std::{env, fs};
use csv::{Writer, Reader};

static MAX_SPEED: f32 = 50.0; // knots
static RATE: i32 = 10; // seconds
static STOP_SPEED_THR: f32 = 0.5; // knots
static DISTANCE_TO_POI_THR: f32 = 1.0; // nmiles
static HISTORY_SIZE: usize = usize::MAX; // how many records should I keep in mem
static FLOCKS_DISTANCE_THRESHOLD: f32 = 0.3; // nmiles
static FLOCKS_MAX_DT_THRESHOLD: i32 = 30 * 60; // seconds
static FLOCKS_MAX_BEARING_THRESHOLD: f32 = 20.0;
static COMP_THR: f32 = 0.1;
static OPW_EPSILON: f32 = 0.0003;
static MODEL_PATH: &str = "vrf_brest_proto_jit_trace.pth";

fn main(){
    let LIMIT = 10000000;
    let KEEP = 1;
    let TO = 30;
    let SLEEP = 1;

    let args = env::args().collect::<Vec<_>>();

    let attack_arg = args[1].to_lowercase();
    let input = args[2].to_lowercase();
    let output_risks = args[3].to_lowercase();

    let mut knowledge = None;
    let mut targets : Option<Vec<i32>> = None;
    let mut precision = None;

    if args.len() > 4 && args[4].to_lowercase() != "none" {
        knowledge = Some(args[4].parse::<i32>().unwrap());
    }

    if args.len() > 5 && args[5].to_lowercase() != "none" {
        targets = Some(args[5].split(",").map(|x|->i32{x.parse().unwrap()}).collect());
    }
    
    if args.len() > 6 && args[6].to_lowercase() != "none" {
        precision = Some(args[6].to_lowercase());
    }

    let get_url = format!("http://localhost:8080?id={}&limit={}&keep={}", input, LIMIT, KEEP);
    let put_url_risks = format!("http://localhost:8080?id={}", output_risks);

    let batch: String = get_http(&get_url, 1, TO).expect("Connection dropped");
    
    let mut oid_vec : Vec<i32> = vec![];
    let mut tms_vec : Vec<i32> = vec![];
    let mut lon_vec : Vec<f32> = vec![];
    let mut lat_vec : Vec<f32> = vec![];
    let mut stop_vec : Vec<i32> = vec![];

    for l in batch.lines() {
        let s : Vec<&str> = l.split(",").collect();
        oid_vec.push(s[0].parse::<i32>().unwrap());
        lon_vec.push(s[1].parse::<f32>().unwrap());
        lat_vec.push(s[2].parse::<f32>().unwrap());
        tms_vec.push(s[3].parse::<i32>().unwrap());
        stop_vec.push(s[5].parse::<i32>().unwrap());
    }

    let df = df!(
            "oid" => oid_vec,
            "tms" => tms_vec,
            "lon" => lon_vec,
            "lat" => lat_vec,
            "stop" => stop_vec
        ).unwrap();

    let mask = col("stop").eq(1);
    let trajdf : TrajDataFrame = TrajDataFrame::new_from_df(df.lazy().filter(mask).collect().unwrap());

    let risks;
    if attack_arg == "homework".to_string() {
        let at = HomeWorkAttack::new(knowledge.unwrap_or(2));
        risks = at.assess_risk(trajdf, targets);
    }
    else if attack_arg == "location".to_string() {
        let at = LocationAttack::new(knowledge.unwrap_or(2));
        risks = at.assess_risk(trajdf, targets);
    }
    else if attack_arg == "locationtime".to_string() {
        let at = LocationTimeAttack::new(knowledge.unwrap_or(2), precision);
        risks = at.assess_risk(trajdf, targets);
    }
    else if attack_arg == "locationsequence".to_string() {
        let at = LocationSequenceAttack::new(knowledge.unwrap_or(2));
        risks = at.assess_risk(trajdf, targets);
    }
    else if attack_arg == "uniquelocation".to_string() {
        let at = UniqueLocationAttack::new(knowledge.unwrap_or(2));
        risks = at.assess_risk(trajdf, targets);
    }
    else {
        panic!("{}", format!("No attack with name '{}' exists.", attack_arg));
    }

    let s1 = risks.column("oid").unwrap();
    let s2 = risks.column("risk").unwrap();

    let mut result : String = String::from("");

    for (id, risk) in s1.iter().zip(s2.iter()) {
        result.push_str(&format!("{},{}\n", id,risk));
    }

    put_http(result.clone(), &put_url_risks);
}