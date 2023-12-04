use crate::structs::{Coordinate, Pois, Record, TrajCollection, Trajectory, TrajDataFrame};
use polars::prelude::*;

pub fn filter_trajdf(tdf: TrajDataFrame, max_speed_kmh: Option<f32>, max_lat:Option<f32>, min_lat:Option<f32>, max_lon:Option<f32>, min_lon:Option<f32>) -> TrajDataFrame {
    
    let max_speed_kmh = max_speed_kmh.unwrap_or(200.);
    let max_lat = max_lat.unwrap_or(90.);
    let min_lat = min_lat.unwrap_or(-90.);
    let max_lon = max_lon.unwrap_or(180.);
    let min_lon = min_lon.unwrap_or(-180.);
    
    let mask = col("speed").gt_eq(0.).and(col("speed").lt_eq(max_speed_kmh).and(col("lat").gt_eq(min_lat).and(col("lat").lt_eq(max_lat).and(col("lon").gt_eq(min_lon).and(col("lon").lt_eq(max_lon))))));
    TrajDataFrame{df: tdf.df.lazy().filter(mask).collect().unwrap()}
}

pub fn filter_record(record: Record, max_speed_kmh: Option<f32>, max_lat:Option<f32>, min_lat:Option<f32>, max_lon:Option<f32>, min_lon:Option<f32>) {
    
}