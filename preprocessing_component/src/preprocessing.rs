use crate::structs::{Coordinate, Record, TrajCollection, Trajectory, TrajDataFrame, SimpleTrajCollection, SimpleTrajectory};
use polars::prelude::*;
use dbscan::Classification::*;
use dbscan::Model;

pub fn calculate_speed(record: Record, traj_coll: &SimpleTrajCollection) -> SimpleTrajectory {
    if !traj_coll.object.contains_key(&record.oid) {
        return SimpleTrajectory::new(
            record.oid,
            crate::HISTORY_SIZE,
            Coordinate {
                x: record.lon,
                y: record.lat,
            },
            record.t,
        );
    }

    let oid_traj = traj_coll.object.get(&record.oid).unwrap();

    let mut new_traj = SimpleTrajectory::new_empty(record.oid, crate::HISTORY_SIZE);
    let coord = Coordinate {
        x: record.lon,
        y: record.lat,
    };

    if record.t == oid_traj.timestamps.last().unwrap().to_owned() {
        return new_traj;
    };

    let speed_now = oid_traj.calculate_speed(&coord, &record.t);

    if speed_now > crate::MAX_SPEED {
        return new_traj;
    };

    new_traj.insert_unbounded(
        coord.clone(),
        record.t,
        speed_now
    );

    return new_traj;
}

pub fn filter_trajdf(df: DataFrame, max_speed_kmh: Option<f32>, max_lat:Option<f32>, min_lat:Option<f32>, max_lon:Option<f32>, min_lon:Option<f32>) -> DataFrame {
    let max_speed_kmh = max_speed_kmh.unwrap_or(crate::MAX_SPEED);
    let max_lat = max_lat.unwrap_or(90.);
    let min_lat = min_lat.unwrap_or(-90.);
    let max_lon = max_lon.unwrap_or(180.);
    let min_lon = min_lon.unwrap_or(-180.);
    
    let mask = col("speed").lt_eq(max_speed_kmh).and(col("lat").gt_eq(min_lat).and(col("lat").lt_eq(max_lat).and(col("lon").gt_eq(min_lon).and(col("lon").lt_eq(max_lon)))));
    df.lazy().filter(mask).collect().unwrap()
}

pub fn stop_detection(mut df: DataFrame, speed_threshold: Option<f32>) ->  DataFrame {

    let speed_threshold = polars::prelude::AnyValue::Float32(speed_threshold.unwrap_or(crate::STOP_SPEED_THR));

    let d = df.column("speed").unwrap()
        .iter()
        .map(|s| {
            if s <= speed_threshold {
                1
            }
            else{
                0
            }
        })
        .collect::<Vec<i32>>();
    
    (*df.with_column(Series::new("stop", d)).unwrap()).clone().into()
}

pub fn clustering(mut df : DataFrame, eps: Option<f32>, mpt: Option<f32>) -> DataFrame {
    let eps = eps.unwrap_or(0.0001);
    let mpt = mpt.unwrap_or(3.0) as usize;
    
    let model : Model<f32> = Model::new(eps.into(), mpt);

    let lons = df.column("lon").unwrap();
    let lats = df.column("lat").unwrap();

    let mut inputs : Vec<Vec<f32>> = vec![];

    for (x, y) in lons.iter().zip(lats.iter()) {
        inputs.push(vec![x.try_extract().unwrap(), y.try_extract().unwrap()]);
    }

    let output = model.run(&inputs);

    let mut lon_final_coordinates : Vec<f32> = vec![];
    let mut lat_final_coordinates : Vec<f32> = vec![];

    for (enumeration_index, i) in output.iter().enumerate() {
        match i {
            Core(idx) => {
                lon_final_coordinates.push(inputs[*idx][0]);
                lat_final_coordinates.push(inputs[*idx][1]);
            },
            Edge(idx) => {
                lon_final_coordinates.push(inputs[*idx][0]);
                lat_final_coordinates.push(inputs[*idx][1]);
            },
            Noise => {
                lon_final_coordinates.push(inputs[enumeration_index][0]);
                lat_final_coordinates.push(inputs[enumeration_index][1]);
            }
        }
    }

    let lon_final : Series = Series::new("lon", &lon_final_coordinates);
    let lat_final : Series = Series::new("lat", &lat_final_coordinates);

    let df = df.replace("lon", lon_final).unwrap();
    let df = df.replace("lat", lat_final).unwrap();

    return df.clone();
}