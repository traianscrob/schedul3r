use chrono::{DateTime, Duration, Local};
use std::sync::{Arc, Mutex};
use timer::Timer;
use std::sync::mpsc::channel;
use std::{any::type_name, thread};
use dict::{ Dict, DictIface };
use crossbeam_utils::thread as other_thread;

//types
pub type Callback = dyn IExecutionResult<()> + Send + 'static + Sync;
type TimerCallback = dyn FnMut() + Send + Sync + 'static;

//traits
pub trait Job : Copy{
    fn execute();
}

pub trait IExecutionResult<T: Sized + Copy + 'static>{
    fn get_job_name(&self) -> String;
    fn get_start_time(&self) -> DateTime<Local>;
    fn get_run_successfuly(&self) -> Option<bool>;
    fn get_is_running(&self) -> bool;
    fn get_is_cancelling(&self) -> bool;
    fn get_duration(&self) -> Option<Duration>;
}

//structs
pub struct ChronExpression{
    expression: &'static str
}

pub struct JobItem{
    name: String,
    next_run: DateTime<Local>
}

pub struct JobScheduler<'a>{
    _timers: Dict::<Mutex<Box<JobTimer<'a>>>>,
    _handles: Vec<std::thread::JoinHandle<()>>,
    _executions: Vec<Box<dyn IExecutionResult<()> + 'a + Send + Sync>>
}

#[derive(Copy)]
pub struct AfterExecutionResult{
    name: &'static str,
    start_time: DateTime<Local>,
    run_successfuly: Option<bool>,
    is_running: bool,
    is_cancelling: bool,
    duration: Option<Duration>
}

struct JobTimer<'a>{
    name: &'a str,
    cron_expression: &'static str,
    functions: Vec<Box<TimerCallback>>,
    callbacks: Vec<Box<dyn FnMut(Box<AfterExecutionResult>) + Send + Sync>>,
    executions: Vec<Box<dyn IExecutionResult<()> + 'a + Send + Sync>>
}

pub struct ScheduleManager{ }

//implementation
impl JobItem{
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn next_run(&self) -> DateTime<Local> {
        self.next_run.clone()
    }
}

impl ChronExpression{
    pub fn parse(expression: &'static str) -> Self{
        Self{
            expression
        }
    }

    pub fn next(&self) -> DateTime<Local>{
        self.next_for(Local::now()).unwrap()
    }

    pub fn next_for_date(&self, date: DateTime<Local>) -> DateTime<Local>{
        self.next_for(date).unwrap()
    }

    pub fn start(&self) -> Duration{
        let next = self.next_for(Local::now()).unwrap();
        next - Local::now()
    }

    pub fn duration(&self) -> Option<Duration>{
        let next = self.next_for(Local::now()).unwrap();
        let next_next = self.next_for(next).unwrap();

        Some(next_next - next)
    }

    fn next_for(&self, date: DateTime<Local>) -> Result<DateTime<Local>, &'static str>{
        if let Ok(next) = cron_parser::parse(&*self.expression, &date) {
            return Ok(next)
        }
        else{
            return Err("Error parsing the cron expression")
        }
    }
}

impl AfterExecutionResult{
    pub fn new(name: &'static str, start_time: DateTime<Local>, duration: chrono::Duration, run_successfuly: bool) -> Self{
        Self{
            name,
            start_time,
            run_successfuly: Some(run_successfuly),
            is_running: false,
            is_cancelling: false,
            duration: Some(duration)
        }
    }
}

impl IExecutionResult<()> for AfterExecutionResult{
    fn get_job_name(&self) -> String{
        String::from(self.name)
    }

    fn get_start_time(&self) -> DateTime<Local>{
        self.start_time.clone()
    }

    fn get_run_successfuly(&self) -> Option<bool>{
        self.run_successfuly.clone()
    }

    fn get_is_running(&self) -> bool{
        self.is_running
    }

    fn get_is_cancelling(&self) -> bool{
        self.is_cancelling
    }

    fn get_duration(&self) -> Option<Duration>{
        self.duration.clone()
    }
 }

impl Clone for AfterExecutionResult{
    fn clone(&self) -> Self{
        *self
    }
}

impl JobTimer<'static>{
    pub fn new(name: &'static str, cron_expression: &'static str, function: Box<TimerCallback>, callback: Option<Box<dyn FnMut(Box<AfterExecutionResult>) + Send + Sync + 'static>>) -> Self{
        let mut obj = Self {
            name: name,
            cron_expression: cron_expression.clone(),
            functions: vec![],
            callbacks: vec![],
            executions: vec![]
        };

        obj.functions.push(function);

        match callback {
            Some(call) => {
                obj.callbacks.push(call);
            },
            None => {}
        }

        obj
    }

    fn schedule<F: FnMut(Box<AfterExecutionResult>) + Send + 'static + Sync>(name: &'static str, cron_expression: &'static str,
         callback: &mut Box<TimerCallback>,
         after_execute: &mut F){
            let _ = other_thread::scope(|scope| {
                let (_sender, _receiver) = channel::<Duration>();
                let _sender = Arc::new(Mutex::new(_sender));
                let _receiver = Arc::new(Mutex::new(_receiver));
    
                let _j1 = scope.spawn(move |_| {
                    let cron_exp = ChronExpression::parse(cron_expression.clone());
                    let _timer = Timer::new();
                    let (tx, rx) = channel();
                    let _guard = _timer.schedule(cron_exp.next(), cron_exp.duration(), move || {
                        let _ignored = tx.send(());
                    });
    
                    let clone = _sender.clone();
                    loop {
                        if let Ok(_) = rx.recv() {
                            let start = Local::now();
                            
                            callback();
    
                            if let Ok(mutex) = clone.lock(){
                                let _ = mutex.send(Local::now() - start);
                            }
                        }
                    }
                });
    
                let _j2 = scope.spawn(move |_| {
                    let clone = _receiver.clone();
                    loop{
                        if let Ok(mutex) = clone.lock() {
                            if let Ok(work) = mutex.recv() {
                                after_execute(Box::new(AfterExecutionResult::new(name, Local::now(), work, true)));
                            }
                        }
                    }
                });
            });
    }

    pub fn trigger(&mut self){
        let _ = other_thread::scope(|scope|{
            let _ = scope.spawn(move |_| {
                let start = Local::now();
                self.functions[0]();

                let exec_result = AfterExecutionResult::new(self.name, Local::now(), Local::now() - start, true);
                self.executions.push(Box::new(exec_result));

                if !self.callbacks.is_empty() {
                    self.callbacks[0](Box::new(exec_result));
                }          
            });
        });
    }

    pub fn next_run(&self) -> DateTime<Local>{
        ChronExpression::parse(self.cron_expression.clone()).next()
    }
}

impl JobScheduler<'static>{
    pub fn new() -> Self{
        Self{
            _timers: Dict::<Mutex<Box<JobTimer>>>::new(),
            _handles: vec![],
            _executions: Vec::<Box<dyn IExecutionResult<()> + 'static + Send + Sync>>::new()
        }
    }

    pub fn all(&self) -> Vec<JobItem>{
        let mut vector = Vec::<JobItem>::new();

        for elem in self._timers.iter() {
            let value = elem.val.lock().unwrap();

            vector.push(JobItem{
                name: elem.key.clone(),
                next_run: value.next_run(),
            });
        }

        vector
    }

    pub fn schedule<J: Job>(&mut self, cron_expression: &'static str) -> Result<bool, String>{
        let function = || J::execute();
        let callback = |_| {};

        self.private_schedule(type_name::<J>(), cron_expression, function, Some(callback))
    }

    pub fn schedule_with_callback<Func: FnMut() + Send + Sync + 'static + std::marker::Copy,
         Callback: FnMut(Box<AfterExecutionResult>) + Send + Sync + 'static + Copy>(&mut self,
        name: &'static str,
        cron_expression: &'static str,
        function: Func,
        callback: Option<Callback>) -> Result<bool, String>{
        self.private_schedule(name, cron_expression, function, callback)
    }

    pub fn execute<Func: FnOnce() + Send + Sync + 'static + std::marker::Copy>(&mut self, function: Func){
        thread::spawn(move || {
            function();
        });
    }

    pub fn trigger<J: Job>(&mut self){
        self.private_trigger(type_name::<J>());
    }

    pub fn trigger_by_name(&mut self, name: &'static str){
        self.private_trigger(name);
    }

    fn private_trigger(&mut self, name: &'static str){
        if self._timers.contains_key(name) {
            for elem in self._timers.iter_mut() {
                if elem.key == String::from(name) {
                    let mut value = elem.val.lock().unwrap();
                    value.trigger();
                    break;
                }
            }
        }
    }

    fn private_schedule<Func: FnMut() + Send + Sync + 'static + Copy,
                        CallbackFunc: FnMut(Box<AfterExecutionResult>) + Send + Sync + 'static + Copy>(&mut self,
                            name: &'static str,
                            cron_expression: &'static str,
                            function: Func,
                            callback: Option<CallbackFunc>) -> Result<bool, String> {
        if self._timers.contains_key(name) {
            return Err(format!("There's already a job named: {name}!", name = name));
        }

        let mut func: Box<TimerCallback> = Box::new(function);
        let handler = thread::spawn(move || {
            JobTimer::schedule(name, cron_expression, &mut func, &mut move |data: Box<AfterExecutionResult>|{
                match callback {
                    |None => {},
                    |Some(mut f) => {
                        f(data);
                    }
                };
            });
        });

        let callback_func: Option<Box<dyn FnMut(Box<AfterExecutionResult>) + Send + Sync + 'static>> = match callback {
            None => None,
            Some(call) => Some(Box::new(call))
        };
        
        self._handles.push(handler);
        self._timers.add(String::from(name), Mutex::new(Box::new(JobTimer::new(name, cron_expression, Box::new(function), callback_func))));
        
        Ok(true)
    }
}

impl ScheduleManager{
    pub fn instance() -> JobScheduler<'static> {
        JobScheduler::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::Job;
    use crate::ChronExpression;
    use crate::ScheduleManager;
    use chrono::{DateTime, Local, Timelike, Datelike};

    #[derive(Copy, Clone)]
    struct MyJob{}

    impl Job for MyJob{
        fn execute() { 
            println!("My Job executed!");
        }
    }

    #[derive(Debug, Copy, Clone, Default)]
    pub struct TestCall{
        calls: i16
    }

    impl TestCall{
        pub fn new() -> Self{
            Self{
                calls : 0
            }
        }

        fn increment(&mut self) -> i16 {
            self.calls += 1;

            self.calls
        }
    }

    #[test]
    fn cron_expression_works() {
        let cron_exp = ChronExpression::parse("13 14 * 1 6");
        let date: DateTime<Local> = cron_exp.next();

        assert_eq!(13, date.minute());
        assert_eq!(14, date.hour());
        assert_eq!(1, date.month());
    }

    #[test]
    fn scheduler_works() {
        let mut scheduler = ScheduleManager::instance();
        let mut test = TestCall::new();

        let callback = |_| {
            println!("Callback executed");
        };

        let _job1 = scheduler.schedule_with_callback("test", "*/1 * * * *", move || { }, Some(callback));
        let _job2 = scheduler.execute(move || {
            println!("Executed!");
            assert_eq!(test.increment(), 1);
        });

        let _result = scheduler.schedule::<MyJob>("*/1 * * * *");
        scheduler.trigger::<MyJob>();

        scheduler.trigger_by_name("test");
    }
}
