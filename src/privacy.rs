use polars::{prelude::*, frame::row::Row};
use polars::prelude::Expr;
use itertools::{Itertools, Combinations};
use crate::structs::{Coordinate, Pois, Record, TrajCollection, Trajectory, TrajDataFrame};
use ndarray::iter::Iter;
use std::collections::HashMap;
use crate::DataType::{Int32, Float32, Utf8, Float64};
use chrono::{DateTime, Datelike, Timelike, Utc}; 

pub struct LocationAttack {
    pub knowledge_length: i32,
}

impl LocationAttack {

    pub fn new(k: i32) -> Self{
        if k < 1 {
            panic!("Parameter knowledge_length should not be less than 1");
        }
        LocationAttack {knowledge_length: k}
    }

    pub fn get_knowledge_length(&self) -> i32{
        self.knowledge_length
    }
    
    pub fn set_knowledge_length(&mut self, value:i32){
        if value < 1{
            panic!("Parameter knowledge_length should not be less than 1");
        }
        else{
            self.knowledge_length = value;
        }
    }

    pub fn generate_instances(&self, single_traj:DataFrame) -> Vec<Vec<Vec<f64>>>{
        let size = single_traj.height();
        if self.knowledge_length > size.try_into().unwrap() {
            let vector_of_vectors = single_traj.to_ndarray::<Float64Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f64>>>();
            return vector_of_vectors.into_iter().combinations(size).collect::<Vec<Vec<Vec<_>>>>();
        }
        else {
            let vector_of_vectors = single_traj.to_ndarray::<Float64Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f64>>>();
            return vector_of_vectors.into_iter().combinations(self.knowledge_length.try_into().unwrap()).collect::<Vec<Vec<Vec<_>>>>();
        }
    }

    pub fn all_risks(&self, traj:TrajDataFrame, targets_option:Option<Vec<i32>>) -> DataFrame{
        let targets = targets_option.unwrap_or(vec![]);
        let tdf:TrajDataFrame;

        if targets.len() > 0 {
            tdf = TrajDataFrame::new_from_df(traj.df.clone().lazy().filter(col("oid").is_in(lit(Series::from_iter(targets)))).collect().unwrap());
        }
        else {
            tdf = TrajDataFrame::new_from_df(traj.df.clone());
        }

        let oid_all : Vec<i32> = tdf.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect();
        let mut risks : Vec <f32> = vec![];
        for oid in oid_all.clone() {
            let mask = col("oid").eq(oid);
            let group = tdf.df.clone().lazy().filter(mask).collect().unwrap();
            risks.push(self.risk(group,TrajDataFrame::new_from_df(traj.df.clone())));
        }

        df!(
            "oid" => oid_all,
            "risk" => risks,
        ).unwrap().sort(["oid"], false, false).unwrap()
    }

    pub fn risk(&self, single_traj:DataFrame, traj:TrajDataFrame) -> f32 {
        let instances = self.generate_instances(single_traj);
        let mut risk:f32 = 0.0;

        for instance in instances{
            let mut oid_all : Vec<i32> = traj.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect::<Vec<i32>>();
            oid_all.sort();
            let mut matches : Vec<i32> = vec![];
            for oid in oid_all {
                let mask = col("oid").eq(oid);
                let group = traj.df.clone().lazy().filter(mask).collect().unwrap();
                matches.push(self._match(group, instance.clone()));
            }

            let prob = 1.0 / matches.iter().sum::<i32>() as f32;
            if prob > risk {
                risk = prob
            }
            if risk == 1.0 {
                break
            }
        }

        return risk;
    }

    pub fn assess_risk(&self, traj:TrajDataFrame, targets:Option<Vec<i32>>) -> DataFrame{
        let sorted_traj = TrajDataFrame::new_from_df(traj.df.sort(&["oid","tms"], true, false).unwrap());
        return self.all_risks(sorted_traj, targets);
    }

    pub fn _match(&self, single_traj:DataFrame, traj:Vec<Vec<f64>>) -> i32{
        let locs = single_traj.group_by(["lon","lat"]).unwrap().select(&["tms"]).count().unwrap();
        let instances : Vec<_> = traj
            .into_iter()
            .map(|x| {
                let row_data = x.iter().map(|&val| AnyValue::Float64(val)).collect();
                Row::new(row_data)
            }).collect();
        let inst = DataFrame::from_rows(&instances).unwrap();

        let oid : Vec<i32> = inst.column("column_0").unwrap().cast(&Int32).unwrap().i32().unwrap().into_no_null_iter().collect();
        let lon : Vec<f32> = inst.column("column_1").unwrap().cast(&Float32).unwrap().f32().unwrap().into_no_null_iter().collect();
        let lat : Vec<f32> = inst.column("column_2").unwrap().cast(&Float32).unwrap().f32().unwrap().into_no_null_iter().collect();
        let tms : Vec<i32> = inst.column("column_3").unwrap().cast(&Int32).unwrap().i32().unwrap().into_no_null_iter().collect();

        let inst_cast = df!(
            "oid" => oid,
            "lon" => lon,
            "lat" => lat,
            "tms" => tms
        ).unwrap();
        
        let inst = inst_cast.group_by(["lat", "lon"]).unwrap().select(&["tms"]).count().unwrap();
        let locs_inst = locs.inner_join(&inst, vec!["lat", "lon"], vec!["lat", "lon"]).unwrap();
        
        if locs_inst.height() != inst.height() {
            return 0;
        }
        else {
            let mask = col("tms_count").gt_eq(col("tms_count_right"));
            if locs_inst.lazy().filter(mask).collect().unwrap().height() != inst.height() {
                return 0;
            }
            else {
                return 1;
            }
        }
    }
}

pub struct HomeWorkAttack {
    pub knowledge_length: i32,
}

impl HomeWorkAttack {

    pub fn new(k: i32) -> Self{
        if k < 1 {
            panic!("Parameter knowledge_length should not be less than 1");
        }
        HomeWorkAttack {knowledge_length: k}
    }

    pub fn get_knowledge_length(&self) -> i32{
        self.knowledge_length
    }
    
    pub fn set_knowledge_length(&mut self, value:i32){
        if value < 1{
            panic!("Parameter knowledge_length should not be less than 1");
        }
        else{
            self.knowledge_length = value;
        }
    }

    pub fn generate_instances(&self, single_traj:DataFrame) -> Vec<Vec<f64>>{
        single_traj.head(Some(2)).to_ndarray::<Float64Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f64>>>()
    }

    pub fn all_risks(&self, traj:DataFrame, targets_option:Option<Vec<i32>>) -> DataFrame{
        
        let targets = targets_option.unwrap_or(vec![]);
        let tdf:TrajDataFrame;

        if targets.len() > 0 {
            tdf = TrajDataFrame::new_from_df(traj.clone().lazy().filter(col("oid").is_in(lit(Series::from_iter(targets)))).collect().unwrap());
        }
        else {
            tdf = TrajDataFrame::new_from_df(traj.clone());
        }

        let oid_all : Vec<i32> = tdf.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect();
        let mut risks : Vec <f32> = vec![];
        for oid in oid_all.clone() {
            let mask = col("oid").eq(oid);
            let group = tdf.df.clone().lazy().filter(mask).collect().unwrap();
            risks.push(self.risk(group,TrajDataFrame::new_from_df(traj.clone())));
        }

        df!(
            "oid" => oid_all,
            "risk" => risks,
        ).unwrap().sort(["oid"], false, false).unwrap()
    }

    pub fn risk(&self, single_traj:DataFrame, traj:TrajDataFrame) -> f32 {//, force:Option<bool>) -> RiskReturnType{
        let instances = self.generate_instances(single_traj);
        let mut risk:f32 = 0.0;

        for instance in instances{
            let mut oid_all : Vec<i32> = traj.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect::<Vec<i32>>();
            oid_all.sort();
            let mut matches : Vec<i32> = vec![];
            for oid in oid_all {
                let mask = col("oid").eq(oid);
                let group = traj.df.clone().lazy().filter(mask).collect().unwrap();
                matches.push(self._match(group, instance.clone()));
            }

            let prob = 1.0 / matches.iter().sum::<i32>() as f32;

            if prob > risk {
                risk = prob
            }
            if risk == 1.0 {
                break
            }
        }

        return risk;
    }

    pub fn assess_risk(&self, traj:TrajDataFrame, targets:Option<Vec<i32>>) -> DataFrame {
        let freq = traj.df.group_by(["oid","lon","lat"]).unwrap().count().unwrap();//.sort(&["tms_count","oid"],true,false).unwrap();
        self.all_risks(freq, targets)
    }

    pub fn _match(&self, single_traj:DataFrame, instance:Vec<f64>) -> i32{
        let mask = col("lon").eq(instance[1] as f32).and(col("lat").eq(instance[2] as f32));
        let locs_inst = single_traj.head(Some(2)).lazy().filter(mask).collect().unwrap();

        if locs_inst.height() == 1 {
            return 1;
        }
        else {
            return 0;
        }
    }
}

pub struct LocationTimeAttack {
    pub knowledge_length: i32,
    pub precision: String
}

impl LocationTimeAttack {

    pub fn new(k: i32, time_precision:Option<String>) -> Self{
        if k < 1 {
            panic!("Parameter knowledge_length should not be less than 1");
        }
        let time_precision = time_precision.unwrap_or("hour".to_string());
        if !vec!["year","month","day","hour","minute","second"].contains(&time_precision.as_str()){
            panic!("The time precision must be one of the following: 'year', 'month', 'day', 'hour', 'minute', 'second'.")
        }
        LocationTimeAttack {knowledge_length: k, precision: time_precision}
    }

    pub fn get_knowledge_length(&self) -> i32{
        self.knowledge_length
    }
    
    pub fn set_knowledge_length(&mut self, value:i32){
        if value < 1{
            panic!("Parameter knowledge_length should not be less than 1");
        }
        else{
            self.knowledge_length = value;
        }
    }

    pub fn get_time_precision(&self) -> String {
        self.precision.clone()
    }

    pub fn set_time_precision(&mut self, time_precision:&str){
        if !vec!["year","month","day","hour","minute","second"].contains(&time_precision){
            panic!("The time precision must be one of the following: 'year', 'month', 'day', 'hour', 'minute', 'second'.")
        }
        self.precision = time_precision.to_string();
    }

    fn standardize(&self, num: u32) -> String {
        if num.to_string().len() == 1 {
            return "0".to_owned() + num.to_string().as_str();
        }
        
        num.to_string()
    }

    pub fn date_time_precision(&self, dt: i32, precision: &str) -> String {
        let dt : DateTime<Utc> = DateTime::<Utc>::from_timestamp(dt.into(), 0). expect("The timestamp was invalid.");
        let mut result = String::new();
        match precision {
            "Year" | "year" => {
                result += self.standardize(dt.year().try_into().unwrap()).as_str();
            }
            "Month" | "month" => {
                result += self.standardize(dt.year().try_into().unwrap()).as_str();
                result += self.standardize(dt.month()).as_str();
            }
            "Day" | "day" => {
                result += self.standardize(dt.year().try_into().unwrap()).as_str();
                result += self.standardize(dt.month()).as_str();
                result += self.standardize(dt.day()).as_str();
            }
            "Hour" | "hour" => {
                result += self.standardize(dt.year().try_into().unwrap()).as_str();
                result += self.standardize(dt.month()).as_str();
                result += self.standardize(dt.day()).as_str();
                result += self.standardize(dt.hour()).as_str();
            }
            "Minute" | "minute" => {
                result += self.standardize(dt.year().try_into().unwrap()).as_str();
                result += self.standardize(dt.month()).as_str();
                result += self.standardize(dt.day()).as_str();
                result += self.standardize(dt.hour()).as_str();
                result += self.standardize(dt.minute()).as_str();
            }
            "Second" | "second" => {
                result += self.standardize(dt.year().try_into().unwrap()).as_str();
                result += self.standardize(dt.month()).as_str();
                result += self.standardize(dt.day()).as_str();
                result += self.standardize(dt.hour()).as_str();
                result += self.standardize(dt.minute()).as_str();
                result += self.standardize(dt.second()).as_str();
            },
            &_ => panic!("Incorrect type of precision specified.")
        }
        result
    }

    pub fn generate_instances(&self, single_traj:DataFrame) -> Vec<Vec<Vec<f64>>>{
        let size = single_traj.height();
        if self.knowledge_length > size.try_into().unwrap() {
            let vector_of_vectors = single_traj.to_ndarray::<Float64Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f64>>>();
            return vector_of_vectors.into_iter().combinations(size).collect::<Vec<Vec<Vec<_>>>>();
        }
        else {
            let vector_of_vectors = single_traj.to_ndarray::<Float64Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f64>>>();
            return vector_of_vectors.into_iter().combinations(self.knowledge_length.try_into().unwrap()).collect::<Vec<Vec<Vec<_>>>>();
        }
    }

    pub fn all_risks(&self, traj:TrajDataFrame, targets_option:Option<Vec<i32>>) -> DataFrame{
        let targets = targets_option.unwrap_or(vec![]);
        let tdf:TrajDataFrame;

        if targets.len() > 0 {
            tdf = TrajDataFrame::new_from_df(traj.df.clone().lazy().filter(col("oid").is_in(lit(Series::from_iter(targets)))).collect().unwrap());
        }
        else {
            tdf = TrajDataFrame::new_from_df(traj.df.clone());
        }

        let oid_all : Vec<i32> = tdf.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect();
        let mut risks : Vec <f32> = vec![];
        for oid in oid_all.clone() {
            let mask = col("oid").eq(oid);
            let group = tdf.df.clone().lazy().filter(mask).collect().unwrap();
            risks.push(self.risk(group,TrajDataFrame::new_from_df(traj.df.clone())));
        }

        df!(
            "oid" => oid_all,
            "risk" => risks,
        ).unwrap().sort(["oid"], false, false).unwrap()
    }

    pub fn risk(&self, single_traj:DataFrame, traj:TrajDataFrame) -> f32 {
        let instances = self.generate_instances(single_traj);
        let mut risk:f32 = 0.0;

        for instance in instances{
            let mut oid_all : Vec<i32> = traj.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect::<Vec<i32>>();
            oid_all.sort();
            let mut matches : Vec<i32> = vec![];
            for oid in oid_all {
                let mask = col("oid").eq(oid);
                let group = traj.df.clone().lazy().filter(mask).collect().unwrap();
                matches.push(self._match(group, instance.clone()));
            }

            let prob = 1.0 / matches.iter().sum::<i32>() as f32;
            if prob > risk {
                risk = prob
            }
            if risk == 1.0 {
                break
            }
        }

        return risk;
    }

    pub fn assess_risk(&self, traj:TrajDataFrame, targets:Option<Vec<i32>>) -> DataFrame{
        let mut sorted_traj = TrajDataFrame::new_from_df(traj.df.sort(&["oid","tms"], true, false).unwrap());
        let temp : Vec<_> = sorted_traj.df
                            .column("tms").unwrap()
                            .i32().unwrap()
                            .into_iter()
                            .map(|x| self.date_time_precision(x.unwrap(), &self.precision))
                            .collect();
        sorted_traj.df.with_column(Series::new("temp", temp));
        
        return self.all_risks(sorted_traj, targets);
    }

    pub fn _match(&self, single_traj:DataFrame, traj:Vec<Vec<f64>>) -> i32 {
        let locs = single_traj.group_by(["lon", "lat", "temp"]).unwrap().select(&["tms"]).count().unwrap();
        let instances : Vec<_> = traj
            .into_iter()
            .map(|x| {
                let row_data = x.iter().map(|&val| AnyValue::Float64(val)).collect();
                Row::new(row_data)
            }).collect();
        let inst = DataFrame::from_rows(&instances).unwrap();

        let oid : Vec<i32> = inst.column("column_0").unwrap().cast(&Int32).unwrap().i32().unwrap().into_no_null_iter().collect();
        let lon : Vec<f32> = inst.column("column_1").unwrap().cast(&Float32).unwrap().f32().unwrap().into_no_null_iter().collect();
        let lat : Vec<f32> = inst.column("column_2").unwrap().cast(&Float32).unwrap().f32().unwrap().into_no_null_iter().collect();
        let tms : Vec<i32> = inst.column("column_3").unwrap().cast(&Int32).unwrap().i32().unwrap().into_no_null_iter().collect();
        let binding = inst.column("column_4").unwrap().cast(&Int32).unwrap().cast(&Utf8).unwrap();
        let temp : Vec<&str> = binding.utf8().unwrap().into_no_null_iter().collect();

        let inst_cast = df!(
                    "oid" => oid,
                    "lon" => lon,
                    "lat" => lat,
                    "tms" => tms,
                    "temp" => temp
                ).unwrap();
        
        let inst = inst_cast.group_by(["lat", "lon", "temp"]).unwrap().select(&["tms"]).count().unwrap();
        let locs_inst = locs.inner_join(&inst, vec!["lat", "lon", "temp"], vec!["lat", "lon", "temp"]).unwrap();

        if locs_inst.height() != inst.height() {
            return 0;
        }
        else {
            let mask = col("tms_count").gt_eq(col("tms_count_right"));
            if locs_inst.lazy().filter(mask).collect().unwrap().height() != inst.height() {
                return 0;
            }
            else {
                return 1;
            }
        }
    }
}

pub struct UniqueLocationAttack {
    pub knowledge_length: i32
}

impl UniqueLocationAttack {

    pub fn new(k: i32) -> Self{
        if k < 1 {
            panic!("Parameter knowledge_length should not be less than 1");
        }
        UniqueLocationAttack {knowledge_length: k}
    }

    pub fn get_knowledge_length(&self) -> i32{
        self.knowledge_length
    }
    
    pub fn set_knowledge_length(&mut self, value:i32){
        if value < 1{
            panic!("Parameter knowledge_length should not be less than 1");
        }
        else{
            self.knowledge_length = value;
        }
    }

    pub fn generate_instances(&self, single_traj:DataFrame) -> Vec<Vec<Vec<f64>>>{
        let size = single_traj.height();
        if self.knowledge_length > size.try_into().unwrap() {
            let vector_of_vectors = single_traj.to_ndarray::<Float64Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f64>>>();
            return vector_of_vectors.into_iter().combinations(size).collect::<Vec<Vec<Vec<_>>>>();
        }
        else {
            let vector_of_vectors = single_traj.to_ndarray::<Float64Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f64>>>();
            return vector_of_vectors.into_iter().combinations(self.knowledge_length.try_into().unwrap()).collect::<Vec<Vec<Vec<_>>>>();
        }
    }

    pub fn all_risks(&self, traj:DataFrame, targets_option:Option<Vec<i32>>) -> DataFrame{
        let targets = targets_option.unwrap_or(vec![]);
        let tdf:TrajDataFrame;

        if targets.len() > 0 {
            tdf = TrajDataFrame::new_from_df(traj.clone().lazy().filter(col("oid").is_in(lit(Series::from_iter(targets)))).collect().unwrap());
        }
        else {
            tdf = TrajDataFrame::new_from_df(traj.clone());
        }

        let oid_all : Vec<i32> = tdf.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect();
        let mut risks : Vec <f32> = vec![];
        for oid in oid_all.clone() {
            let mask = col("oid").eq(oid);
            let group = tdf.df.clone().lazy().filter(mask).collect().unwrap();
            risks.push(self.risk(group,TrajDataFrame::new_from_df(traj.clone())));
        }

        df!(
            "oid" => oid_all,
            "risk" => risks,
        ).unwrap().sort(["oid"], false, false).unwrap()
    }

    pub fn risk(&self, single_traj:DataFrame, traj:TrajDataFrame) -> f32 {
        let instances = self.generate_instances(single_traj);
        let mut risk:f32 = 0.0;

        for instance in instances{
            let mut oid_all : Vec<i32> = traj.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect::<Vec<i32>>();
            oid_all.sort();
            let mut matches : Vec<i32> = vec![];
            for oid in oid_all {
                let mask = col("oid").eq(oid);
                let group = traj.df.clone().lazy().filter(mask).collect().unwrap();
                matches.push(self._match(group, instance.clone()));
            }

            let prob = 1.0 / matches.iter().sum::<i32>() as f32;
            if prob > risk {
                risk = prob
            }
            if risk == 1.0 {
                break
            }
        }

        return risk;
    }

    pub fn assess_risk(&self, traj:TrajDataFrame, targets:Option<Vec<i32>>) -> DataFrame{
        let freq = traj.df.group_by(["oid","lon","lat"]).unwrap().count().unwrap().sort(&["tms_count","oid"],true,false).unwrap();
        self.all_risks(freq, targets)
    }

    pub fn _match(&self, single_traj:DataFrame, traj:Vec<Vec<f64>>) -> i32{
        let instances : Vec<_> = traj
            .into_iter()
            .map(|x| {
                let row_data = x.iter().map(|&val| AnyValue::Float64(val)).collect();
                Row::new(row_data)
            }).collect();
        let inst = DataFrame::from_rows(&instances).unwrap();

        let lon : Vec<f32> = inst.column("column_1").unwrap().cast(&Float32).unwrap().f32().unwrap().into_no_null_iter().collect();
        let lat : Vec<f32> = inst.column("column_2").unwrap().cast(&Float32).unwrap().f32().unwrap().into_no_null_iter().collect();

        let inst = df!(
            "lon" => lon,
            "lat" => lat
        ).unwrap();
        
        let locs_inst = single_traj.inner_join(&inst, vec!["lat", "lon"], vec!["lat", "lon"]).unwrap();
        
        if locs_inst.height() == inst.height() {
            return 1;
        }
        else {
            return 0;
        }
    }
}

pub struct LocationSequenceAttack {
    pub knowledge_length: i32,
}

impl LocationSequenceAttack {

    pub fn new(k: i32) -> Self{
        if k < 1 {
            panic!("Parameter knowledge_length should not be less than 1");
        }
        LocationSequenceAttack {knowledge_length: k}
    }

    pub fn get_knowledge_length(&self) -> i32{
        self.knowledge_length
    }
    
    pub fn set_knowledge_length(&mut self, value:i32){
        if value < 1{
            panic!("Parameter knowledge_length should not be less than 1");
        }
        else{
            self.knowledge_length = value;
        }
    }

    pub fn generate_instances(&self, single_traj:DataFrame) -> Vec<Vec<Vec<f32>>>{
        let size = single_traj.height();
        if self.knowledge_length > size.try_into().unwrap() {
            let vector_of_vectors = single_traj.to_ndarray::<Float32Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f32>>>();
            return vector_of_vectors.into_iter().combinations(size).collect::<Vec<Vec<Vec<_>>>>();
        }
        else {
            let vector_of_vectors = single_traj.to_ndarray::<Float32Type>(IndexOrder::Fortran).unwrap().outer_iter().map(|row| row.to_vec()).collect::<Vec<Vec<f32>>>();
            return vector_of_vectors.into_iter().combinations(self.knowledge_length.try_into().unwrap()).collect::<Vec<Vec<Vec<_>>>>();
        }
    }

    pub fn all_risks(&self, traj:TrajDataFrame, targets_option:Option<Vec<i32>>) -> DataFrame{
        let targets = targets_option.unwrap_or(vec![]);
        let tdf:TrajDataFrame;

        if targets.len() > 0 {
            tdf = TrajDataFrame::new_from_df(traj.df.clone().lazy().filter(col("oid").is_in(lit(Series::from_iter(targets)))).collect().unwrap());
        }
        else {
            tdf = TrajDataFrame::new_from_df(traj.df.clone());
        }

        let oid_all : Vec<i32> = tdf.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect();
        let mut risks : Vec <f32> = vec![];
        for oid in oid_all.clone() {
            let mask = col("oid").eq(oid);
            let group = tdf.df.clone().lazy().filter(mask).collect().unwrap();
            risks.push(self.risk(group,TrajDataFrame::new_from_df(traj.df.clone())));
        }

        df!(
            "oid" => oid_all,
            "risk" => risks,
        ).unwrap().sort(["oid"], false, false).unwrap()
    }

    pub fn risk(&self, single_traj:DataFrame, traj:TrajDataFrame) -> f32 {
        let instances = self.generate_instances(single_traj);
        let mut risk:f32 = 0.0;

        for instance in instances{
            let mut oid_all : Vec<i32> = traj.df.column("oid").unwrap().unique().unwrap().i32().unwrap().into_no_null_iter().collect::<Vec<i32>>();
            oid_all.sort();
            let mut matches : Vec<i32> = vec![];
            for oid in oid_all {
                let mask = col("oid").eq(oid);
                let group = traj.df.clone().lazy().filter(mask).collect().unwrap();
                matches.push(self._match(group, instance.clone()));
            }

            let prob = 1.0 / matches.iter().sum::<i32>() as f32;
            if prob > risk {
                risk = prob
            }
            if risk == 1.0 {
                break
            }
        }

        return risk;
    }

    pub fn assess_risk(&self, traj:TrajDataFrame, targets:Option<Vec<i32>>) -> DataFrame{
        let sorted_traj = TrajDataFrame::new_from_df(traj.df.sort(&["oid","tms"], true, false).unwrap());
        return self.all_risks(sorted_traj, targets);
    }

    pub fn _match(&self, single_traj:DataFrame, traj:Vec<Vec<f32>>) -> i32{
        let lats : Series = single_traj.column("lat").unwrap().clone();
        let lons : Series = single_traj.column("lon").unwrap().clone();

        let mut count = 0;
        let inst_len = traj.len();

        for idx in 0..single_traj.height(){
            if count >= inst_len{
                break;
            }
            if lons.get(idx).unwrap() == AnyValue::Float32(traj[count][1]) && lats.get(idx).unwrap() == AnyValue::Float32(traj[count][2]) {
                count += 1;
            }
        }

        if inst_len == count{
            return 1;
        }
        else{
            return 0;
        }
    }
}