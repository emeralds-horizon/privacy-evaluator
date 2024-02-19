use itertools::izip;
use libm::atan2f;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use polars::prelude::*;
use polars::frame::DataFrame;

#[derive(Deserialize, Clone)]
pub struct Record {
    pub oid: i32,
    pub t: i32,
    pub lon: f32,
    pub lat: f32,
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Record {{oid: {}, t: {}, lon: {}, lat: {}}}",
            self.oid, self.t, self.lon, self.lat
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Coordinate {
    pub x: f32,
    pub y: f32,
}
impl Coordinate {
    pub fn haversine(&self, coord: &Coordinate) -> f32 {
        let R = 6371000.0;
        let d_lat = (coord.y - self.y).to_radians();
        let d_lon = (coord.x - self.x).to_radians();

        let a: f32 = ((d_lat / 2.0).sin()) * ((d_lat / 2.0).sin())
            + ((d_lon / 2.0).sin())
                * ((d_lon / 2.0).sin())
                * (self.y.to_radians().cos())
                * (coord.y.to_radians().cos());
        let c: f32 = 2.0 * ((a.sqrt()).atan2((1.0 - a).sqrt()));

        R * c / 1000.0 // returns nautical miles
    }

    pub fn from_tuple(tup: (f32, f32)) -> Coordinate {
        Coordinate { x: tup.0, y: tup.1 }
    }

    pub fn bearing(&self, coord: &Coordinate) -> f32 {
        let d_lon = coord.x - self.x;
        let x = coord.y.to_radians().cos() * d_lon.to_radians().sin();
        let y = self.y.to_radians().cos() * coord.y.to_radians().sin()
            - self.y.to_radians().sin() * coord.y.to_radians().cos() * d_lon.to_radians().cos();
        // 	brgs.append(np.degrees(np.arctan2(x,y)))
        atan2f(x, y).to_degrees()
    }

    pub fn extrapolate(&self, speed: f32, bearing: f32, dt: i32) -> Coordinate {
        let distance_m_per_s = speed * 1852.0 / 3600.0 * dt as f32;

        let lon1 = self.x.to_radians();
        let lat1 = self.y.to_radians();

        let rad_bearing = bearing.to_radians();

        let delta = distance_m_per_s / 6371000.0;

        let lat2 = (lat1.sin() * delta.cos() + lat1.cos() * delta.sin() * rad_bearing.cos()).asin();

        let lon2 = lon1
            + atan2f(
                rad_bearing.sin() * delta.sin() * lat1.cos(),
                delta.cos() - lat1.sin() * lat2.sin(),
            );

        Coordinate {
            x: (lon2.to_degrees() + 540.0) % 360.0 - 180.0,
            y: lat2.to_degrees(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Trajectory {
    pub oid: i32,
    pub max_size: usize,
    pub coordinates: Vec<Coordinate>,
    pub timestamps: Vec<i32>,
    pub speed: Vec<f32>,
    pub bearing: Vec<f32>,
    pub stoped: Vec<i8>,
    pub trips: Vec<i32>,
    pub pois: Vec<i32>,
    pub gps: Vec<Vec<i32>>,
}

impl Trajectory {
    pub fn new(oid: i32, max_size: usize, coord: Coordinate, timestamp: i32) -> Trajectory {
        Trajectory {
            oid,
            max_size,
            coordinates: vec![coord.clone()],
            timestamps: vec![timestamp],
            speed: vec![-1.0],
            bearing: vec![-1.0],
            stoped: vec![-1],
            trips: vec![0],
            pois: vec![-1],
            gps: vec![vec![]],
        }
    }

    pub fn new_empty(oid: i32, max_size: usize) -> Trajectory {
        Trajectory {
            oid,
            max_size,
            coordinates: vec![],
            timestamps: vec![],
            speed: vec![],
            bearing: vec![],
            stoped: vec![],
            trips: vec![],
            pois: vec![],
            gps: vec![],
        }
    }

    pub fn insert_unbounded(
        &mut self,
        coord: Coordinate,
        ts: i32,
        sp: f32,
        br: f32,
        poi_id: i32,
        trip_id: i32,
        stoped: i8,
        gps: Vec<i32>,
    ) {
        self.speed.push(sp);
        self.bearing.push(br);
        self.coordinates.push(coord.clone());
        self.timestamps.push(ts);
        self.pois.push(poi_id);
        self.stoped.push(stoped);
        self.trips.push(trip_id);
        self.gps.push(gps);
    }

    pub fn extend(&mut self, trajectory: Trajectory) {
        self.speed.extend(trajectory.speed);
        self.bearing.extend(trajectory.bearing);
        self.coordinates.extend(trajectory.coordinates);
        self.timestamps.extend(trajectory.timestamps);
        self.pois.extend(trajectory.pois);
        self.stoped.extend(trajectory.stoped);
        self.trips.extend(trajectory.trips);
        self.gps.extend(trajectory.gps);

        let size = self.speed.len();

        if size > self.max_size {
            self.speed.drain(0..size - self.max_size);
            self.bearing.drain(0..size - self.max_size);
            self.coordinates.drain(0..size - self.max_size);
            self.timestamps.drain(0..size - self.max_size);
            self.stoped.drain(0..size - self.max_size);
            self.trips.drain(0..size - self.max_size);
            self.pois.drain(0..size - self.max_size);
            self.gps.drain(0..size - self.max_size);
        }
    }

    pub fn drop_first_n(&mut self, n: usize) {
        self.speed.drain(0..n);
        self.bearing.drain(0..n);
        self.coordinates.drain(0..n);
        self.timestamps.drain(0..n);
        self.stoped.drain(0..n);
        self.trips.drain(0..n);
        self.pois.drain(0..n);
        self.gps.drain(0..n);
    }

    pub fn to_csv(&self) {
        for i in 0..self.speed.len() {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:?}",
                self.oid,
                self.coordinates[i].x,
                self.coordinates[i].y,
                self.speed[i],
                self.bearing[i],
                self.stoped[i],
                self.trips[i],
                self.timestamps[i],
                self.pois[i],
                self.gps[i]
            )
        }
    }

    pub fn print_row(&self, i: usize) {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:?}",
            self.oid,
            self.coordinates[i].x,
            self.coordinates[i].y,
            self.speed[i],
            self.bearing[i],
            self.stoped[i],
            self.trips[i],
            self.timestamps[i],
            self.pois[i],
            self.gps[i]
        )
    }

    pub fn calculate_speed(&self, coord: &Coordinate, timestamp: &i32) -> f32 {
        self.coordinates.last().unwrap().haversine(coord) * 3600.0
            / (timestamp - self.timestamps.last().unwrap()) as f32
    }

    pub fn calculate_bearing(&self, coord: &Coordinate) -> f32 {
        self.coordinates.last().unwrap().bearing(coord)
    }

    pub fn resample(&self, rate: i32, timestamp: &i32, sp: f32, br: f32) -> Vec<(Coordinate, i32)> {
        // eprintln!("{} - {} - {}", (timestamp-self.last_timestamp)/rate, timestamp, self.last_timestamp);
        let mut coords = vec![];
        for i in 0..(timestamp - self.timestamps.last().unwrap()) / rate {
            let new_coord = self
                .coordinates
                .last()
                .unwrap()
                .extrapolate(sp, br, rate * (i + 1));
            // self.insert_unbounded(new_coord, self.last_timestamp+rate, sp, br);
            coords.push((new_coord, self.timestamps.last().unwrap() + rate * (i + 1)))
        }
        coords
    }

    pub fn extrapolate_next(&self, dt: i32) -> Coordinate {
        self.coordinates.last().unwrap().extrapolate(
            self.speed.last().unwrap().to_owned(),
            self.bearing.last().unwrap().to_owned(),
            dt,
        )
    }

    pub fn OPW_TR(&self, coord: &Coordinate, timestamp: i32) -> Option<usize> {
        fn _calc_SED(
            pnt_s: &Coordinate,
            ts_s: i32,
            pnt_m: &Coordinate,
            ts_m: i32,
            pnt_e: &Coordinate,
            ts_e: i32,
        ) -> f32 {
            let numerator = ts_m - ts_s;
            let denominator = ts_e - ts_s;

            let time_ratio = if denominator != 0 {
                numerator / denominator
            } else {
                1
            };

            let x_value = pnt_s.x + (pnt_e.x - pnt_s.x) * time_ratio as f32;
            let y_value = pnt_s.y + (pnt_e.y - pnt_s.y) * time_ratio as f32;

            ((x_value - pnt_m.x).powi(2) + (y_value - pnt_m.y).powi(2)).sqrt()
        }

        if self.coordinates.len() < 2 {
            return None;
        }

        for (mid_id, (mid_coord, mid_ts)) in self.coordinates[1..]
            .iter()
            .zip(self.timestamps[1..].iter())
            .enumerate()
        {
            let err_sed = _calc_SED(
                self.coordinates.first().unwrap(),
                self.timestamps.first().unwrap().clone(),
                mid_coord,
                mid_ts.to_owned(),
                coord,
                timestamp.clone(),
            );
            if err_sed > crate::OPW_EPSILON {
                return Some(mid_id + 1);
            }
        }
        return None;
    }
}

#[derive(Debug, Clone)]
pub struct TrajCollection {
    pub object: HashMap<i32, Trajectory>,
}

impl std::fmt::Display for TrajCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.object)
    }
}

impl TrajCollection {

    pub fn extend_flush(&mut self, trajectory: Trajectory, n_opt: Option<usize>) {
        match self.object.entry(trajectory.oid) {
            Entry::Vacant(e) => {
                e.insert(trajectory);
            }
            Entry::Occupied(mut e) => {
                e.get_mut().extend(trajectory);
                match n_opt {
                    Some(n) => e.get_mut().drop_first_n(n),
                    _ => (),
                }
            }
        }
    }

    pub fn pretty(&self) {
        for (_, trajec) in self.object.clone().into_iter() {
            for i in 0..trajec.speed.len() {
                println!(
                    "{:.3}\t{:.3}\t{:.3}\t{:.3}\t{}\t{}\t{}",
                    trajec.coordinates[i].x,
                    trajec.coordinates[i].y,
                    trajec.speed[i],
                    trajec.bearing[i],
                    trajec.stoped[i],
                    trajec.trips[i],
                    trajec.timestamps[i]
                )
            }
        }
    }

    pub fn to_csv(&self) {
        println!("oid,lon,lat,speed,bearing,stoped,trip,timestamp,poi_id");
        for (o_id, trajec) in self.object.clone().into_iter() {
            for i in 0..trajec.speed.len() {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    o_id,
                    trajec.coordinates[i].x,
                    trajec.coordinates[i].y,
                    trajec.speed[i],
                    trajec.bearing[i],
                    trajec.stoped[i],
                    trajec.trips[i],
                    trajec.timestamps[i],
                    trajec.pois[i]
                )
            }
        }
    }

    pub fn concat(&self, trajcol: TrajCollection) {
        todo!()
    }
}

pub struct TrajDataFrame {
    pub df : DataFrame,
}

impl TrajDataFrame {
    pub fn new_empty() -> TrajDataFrame {
        TrajDataFrame{ df:DataFrame::default()}
    }

    pub fn new_from_df(dataframe : DataFrame) -> TrajDataFrame {
        TrajDataFrame{df:dataframe}
    }

    pub fn new_adv_from_TrajCollection(trajcol: TrajCollection, advanced: Option<bool>) -> TrajDataFrame {
        let adv:bool = advanced.unwrap_or(false);
        
        let (mut oid_all, mut lat_all, mut lng_all, mut tms_all, mut stop_all, mut speed_all) = (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        for (oid, mut trajectory) in trajcol.object {
            oid_all.extend(vec![oid; trajectory.timestamps.len()]);
            let (lons, lats): (Vec<_>, Vec<_>) = trajectory.coordinates.into_iter().map(|Coordinate { x, y }| (x, y)).unzip();
            lng_all.extend(lons);
            lat_all.extend(lats);
            tms_all.extend(trajectory.timestamps);
        
            if adv {
                stop_all.extend(trajectory.stoped);
                speed_all.extend(trajectory.speed);
            }
        }        

        let df = if adv {
            df!
            (
                "oid" => oid_all.into_iter().map(|x| x as i32).collect::<Vec<i32>>(),
                "lon" => lng_all,
                "lat" => lat_all,
                "tms" => tms_all,
                "stop" => stop_all.into_iter().map(|x| x as i32).collect::<Vec<i32>>(),
                "speed" => speed_all
            )
        } else {
            df!
            (
                "oid" => oid_all.into_iter().map(|x| x as i32).collect::<Vec<i32>>(),
                "lon" => lng_all,
                "lat" => lat_all,
                "tms" => tms_all
            )
        };
        
        return TrajDataFrame { df: df.unwrap() };
    }
}

#[derive(Debug, Clone)]
pub struct SimpleTrajectory {
    pub oid: i32,
    pub max_size: usize,
    pub coordinates: Vec<Coordinate>,
    pub timestamps: Vec<i32>,
    pub speed: Vec<f32>
}

impl SimpleTrajectory {
    pub fn new(oid: i32, max_size: usize, coord: Coordinate, timestamp: i32) -> SimpleTrajectory {
        SimpleTrajectory {
            oid,
            max_size,
            coordinates: vec![coord],
            timestamps: vec![timestamp],
            speed: vec![-1.0],
        }
    }

    pub fn new_empty(oid: i32, max_size: usize) -> SimpleTrajectory {
        SimpleTrajectory {
            oid,
            max_size,
            coordinates: vec![],
            timestamps: vec![],
            speed: vec![]
        }
    }

    pub fn insert_unbounded(
        &mut self,
        coord: Coordinate,
        ts: i32,
        sp: f32
    ) {
        self.coordinates.push(coord);
        self.timestamps.push(ts);
        self.speed.push(sp);
    }

    pub fn extend(&mut self, trajectory: SimpleTrajectory) {
        self.coordinates.extend(trajectory.coordinates);
        self.timestamps.extend(trajectory.timestamps);
        self.speed.extend(trajectory.speed);

        let size = self.speed.len();

        if size > self.max_size {
            self.speed.drain(0..size - self.max_size);
            self.coordinates.drain(0..size - self.max_size);
            self.timestamps.drain(0..size - self.max_size);
        }
    }

    pub fn drop_first_n(&mut self, n: usize) {
        self.speed.drain(0..n);
        self.coordinates.drain(0..n);
        self.timestamps.drain(0..n);
    }

    pub fn to_csv(&self) {
        for i in 0..self.speed.len() {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                self.oid,
                self.coordinates[i].x,
                self.coordinates[i].y,
                self.speed[i],
                self.timestamps[i]
            )
        }
    }

    pub fn print_row(&self, i: usize) {
        println!(
            "{}\t{}\t{}\t{}\t{}",
            self.oid,
            self.coordinates[i].x,
            self.coordinates[i].y,
            self.speed[i],
            self.timestamps[i]
        )
    }

    // TODO: haversine se m
    pub fn calculate_speed(&self, coord: &Coordinate, timestamp: &i32) -> f32 {
        self.coordinates.last().unwrap().haversine(coord) * 3600.0
            / (timestamp - self.timestamps.last().unwrap()) as f32
    }
}

#[derive(Debug, Clone)]
pub struct SimpleTrajCollection {
    pub object: HashMap<i32, SimpleTrajectory>,
}

impl std::fmt::Display for SimpleTrajCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.object)
    }
}

impl SimpleTrajCollection {

    pub fn extend_flush(&mut self, trajectory: SimpleTrajectory, n_opt: Option<usize>) {
        match self.object.entry(trajectory.oid) {
            Entry::Vacant(e) => {
                e.insert(trajectory);
            }
            Entry::Occupied(mut e) => {
                e.get_mut().extend(trajectory);
                match n_opt {
                    Some(n) => e.get_mut().drop_first_n(n),
                    _ => (),
                }
            }
        }
    }

    pub fn to_csv(&self) {
        println!("oid,lon,lat,speed,timestamp");
        for (o_id, trajec) in self.object.clone().into_iter() {
            for i in 0..trajec.speed.len() {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    o_id,
                    trajec.coordinates[i].x,
                    trajec.coordinates[i].y,
                    trajec.speed[i],
                    trajec.timestamps[i]
                )
            }
        }
    }

    pub fn to_df(&self) -> DataFrame {
        let mut oid_final : Vec<i32> = vec![];
        let mut lon_final : Vec<f32> = vec![];
        let mut lat_final : Vec<f32> = vec![];
        let mut speed_final : Vec<f32> = vec![];
        let mut time_final : Vec<i32> = vec![];

        for (o_id, trajec) in self.object.clone().into_iter() {
            let oid_vec = vec![o_id ; trajec.timestamps.len().try_into().unwrap()];
            let mut lon_vec : Vec<f32> = vec![];
            let mut lat_vec : Vec<f32> = vec![];
            let mut speed_vec : Vec<f32> = vec![];
            let mut time_vec : Vec<i32> = vec![];

            for i in 0..trajec.speed.len() {
                lon_vec.push(trajec.coordinates[i].x);
                lat_vec.push(trajec.coordinates[i].y);
                speed_vec.push(trajec.speed[i]);
                time_vec.push(trajec.timestamps[i]);                
            }

            oid_final.extend(oid_vec);
            lon_final.extend(lon_vec);
            lat_final.extend(lat_vec);
            speed_final.extend(speed_vec);
            time_final.extend(time_vec);
        }

        df!(
            "oid" => oid_final,
            "lon" => lon_final,
            "lat" => lat_final,
            "t" => time_final,
            "speed" => speed_final
        ).unwrap()
    }
}