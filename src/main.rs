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

use std::fs::File;

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
    let ID = "example";
    let LIMIT = 1000;
    let KEEP = 1;
    let TO = 30;
    let SLEEP = 1;

    let get_url = format!("http://localhost:8080?id={}&limit={}&keep={}", ID, LIMIT, KEEP);
    let put_url = "http://localhost:8080?id=privacy";

    let args = env::args().collect::<Vec<_>>();

    let attack_arg = args[1].to_lowercase();
    let mut knowledge = None;
    let mut targets : Option<Vec<i32>> = None;
    let mut precision = None;

    if args.len() > 2 && args[2].to_lowercase() != "none" {
        knowledge = Some(args[2].parse::<i32>().unwrap());
    }

    if args.len() > 3 && args[3].to_lowercase() != "none" {
        targets = Some(args[3].split(",").map(|x|->i32{x.parse().unwrap()}).collect());
    }
    
    if args.len() > 4 && args[4].to_lowercase() != "none" {
        precision = Some(args[4].to_lowercase());
    }


    let batch: String = get_http(&get_url, 1, TO).expect("Connection dropped");
    fs::write("temp.csv", batch).expect("Unable to write file");

    let v = vec![
        Field::new("oid", DataType::Int32),
        Field::new("tms", DataType::Int32),
        Field::new("lon", DataType::Float32),
        Field::new("lat", DataType::Float32),
    ];

    let schema = Schema::from_iter(v.into_iter());

    let mut trajdf : TrajDataFrame = TrajDataFrame::new_from_df(
        CsvReader::from_path("temp.csv")
            .unwrap()
            .with_dtypes(Some(Arc::new(schema)))
            .has_header(true).finish().unwrap().select(["oid","lon","lat","tms"]).unwrap()
    );

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

    put_http(result, put_url);
}