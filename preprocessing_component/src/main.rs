mod structs;
mod io_http;
mod preprocessing;
use core::time;
use kdam::tqdm;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Instant;
use structs::{Coordinate, Record, TrajCollection, Trajectory, TrajDataFrame, SimpleTrajCollection, SimpleTrajectory};
use io_http::{put_http, get_http};
use std::thread;
use polars::{prelude::*, lazy::dsl::col};
use std::{env, fs};
use csv::{Writer, Reader};
use preprocessing::{filter_trajdf, calculate_speed, stop_detection, clustering};
use itertools::izip;
use dbscan::Classification::*;
use dbscan::Model;

use std::fs::File;
use std::io::Write;

static STOP_SPEED_THR: f32 = 1.0;
static FLOCKS_DISTANCE_THRESHOLD: f32 = 0.5556;
static FLOCKS_MAX_DT_THRESHOLD: i32 = 30 * 60;
static FLOCKS_MAX_BEARING_THRESHOLD: f32 = 20.0;
static OPW_EPSILON: f32 = 0.0003;
static MAX_SPEED: f32 = 100.0;
static HISTORY_SIZE: usize = usize::MAX; // how many records should I keep in mem

fn unpack_parameters(param_str : &str) -> Option<Vec<Option<f32>>> {
    if param_str == "None" {
        None
    }
    else {
        let r_string = param_str.replace(&['[', ']'], "");
        Some(r_string.split(",").collect::<Vec<&str>>().iter()
            .map(|x| {
                if x == &"none" {
                    None
                }
                else {
                    Some(x.parse::<f32>().unwrap())
                }
            })
            .collect::<Vec<Option<f32>>>()
        )
    }
}

fn df_to_csv(df: DataFrame) -> String {
    let oids : Vec<i32> = df.column("oid").unwrap().i32().unwrap().into_no_null_iter().collect();
    let lons : Vec<f32> = df.column("lon").unwrap().f32().unwrap().into_no_null_iter().collect();
    let lats : Vec<f32> = df.column("lat").unwrap().f32().unwrap().into_no_null_iter().collect();
    let ts : Vec<i32> = df.column("t").unwrap().i32().unwrap().into_no_null_iter().collect();
    let speeds : Vec<f32> = df.column("speed").unwrap().f32().unwrap().into_no_null_iter().collect();
    let stops : Vec<i32> = df.column("stop").unwrap().i32().unwrap().into_no_null_iter().collect();

    let mut string_list : Vec<String> = vec![];

    for row in 0..oids.len(){
        string_list.push(format!("{},{},{},{},{},{}", oids[row].to_string(), lons[row].to_string(), lats[row].to_string(), ts[row].to_string(), speeds[row].to_string(), stops[row].to_string()));
    }

    string_list.join("\n")
}

fn main(){
    let LIMIT = 1000000;
    let KEEP = 1;
    let TO = 30;
    let SLEEP = 1;

    let args = env::args().collect::<Vec<_>>();

    if args.len() != 6 {
        panic!("The parameters provided are wrong or in wrong format.")
    }

    let input = &args[1];
    let output = &args[2];

    let get_url = format!("http://localhost:8080?id={}&limit={}&keep={}", input, LIMIT, KEEP);
    let put_url = format!("http://localhost:8080?id={}", output);

    let filter_parameters = unpack_parameters(&args[3].to_lowercase()).unwrap();
    let stop_parameters = unpack_parameters(&args[4].to_lowercase()).unwrap();
    let clustering_parameters = unpack_parameters(&args[5].to_lowercase()).unwrap();

    let batch: String = get_http(&get_url, 1, TO).expect("Connection dropped");
    
    fs::write("temp.csv", batch).expect("Unable to write file.");

    let reader_traj = csv::Reader::from_path("temp.csv");
    
    let mut traj_coll = SimpleTrajCollection {
        object: HashMap::new(),
    };

    for record in reader_traj.unwrap().deserialize() {
        let record: Record = record.unwrap();
        let clean_traj = calculate_speed(record.clone(), &traj_coll);
        traj_coll.extend_flush(clean_traj, None);
    }

    let mut final_processed_df : DataFrame;

    let mut tdf = traj_coll.to_df();
    tdf = filter_trajdf(tdf, filter_parameters[0], filter_parameters[1], filter_parameters[2], filter_parameters[3], filter_parameters[4]);
    tdf = stop_detection(tdf, stop_parameters[0]);

    let mut final_processed_df = tdf.clone().lazy().filter(col("stop").eq(0)).collect().unwrap();
    let stopped_df = tdf.lazy().filter(col("stop").eq(1)).collect().unwrap();

    let clustered_df = clustering(stopped_df, clustering_parameters[0], clustering_parameters[1]);
    final_processed_df.vstack_mut(&clustered_df).unwrap();

    put_http(df_to_csv(final_processed_df), &put_url);
}
