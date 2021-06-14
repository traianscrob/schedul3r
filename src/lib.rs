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
type ExecutionResultParam = Box<dyn IExecutionResult<()> + Send + 'static + Sync>;

//traits
pub trait Job{
    fn execute();
}

pub trait IExecutionResult<T: ?Sized>{
    fn get_start_time(&self) -> DateTime<Local>;
    fn get_run_successfuly(&self) -> Option<bool>;
    fn get_is_running(&self) -> bool;
    fn get_is_cancelling(&self) -> bool;
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
    _timers: dict::Dict<Arc<Mutex<JobTimer<'a>>>>,
    _handles: Vec<std::thread::JoinHandle<()>>
}

#[derive(Copy)]
pub struct ExecutionResult{
    start_time: DateTime<Local>,
    run_successfuly: Option<bool>,
    is_running: bool,
    is_cancelling: bool,
    duration: Option<Duration>
}

struct JobTimer<'a>{
    cron_expression: &'static str,
    callback: Box<TimerCallback>,
    execution_result: Option<&'a dyn IExecutionResult<()>>
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

impl ExecutionResult{
    pub fn new(start_time: DateTime<Local>, duration: chrono::Duration, run_successfuly: bool) -> Self{
        Self{
            start_time,
            run_successfuly: Some(run_successfuly),
            is_running: false,
            is_cancelling: false,
            duration: Some(duration)
        }
    }
}

impl IExecutionResult<()> for ExecutionResult{
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
 }

impl Clone for ExecutionResult{
    fn clone(&self) -> Self{
        *self
    }
}

impl<'a> JobTimer<'a>{
    pub fn new(cron_expression: &'static str, callback: Box<TimerCallback>) -> Self{
        Self {
            cron_expression: cron_expression.clone(),
            callback: callback,
            execution_result: None
        }
    }

    fn schedule<F: FnMut(ExecutionResultParam) + Send + 'static + Sync,
                    C: FnMut() + Send + 'static + Sync>(cron_expression: &'static str, mut callback: C, after_execute: &mut F){
            let _ = other_thread::scope(|scope| {
                let (_sender, _receiver) = channel::<Box<dyn IExecutionResult<()> + Send + 'static + Sync>>();
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
                    loop{
                        if let Ok(_) = rx.recv() {
                            let start = Local::now();
    
                            callback();
    
                            if let Ok(mutex) = clone.lock(){
                                let _ = mutex.send(Box::new(ExecutionResult::new(Local::now(), Local::now() - start, true)));
                            }
                        }
                    }
                });
    
                let _j2 = scope.spawn(move |_| {
                    let clone = _receiver.clone();
                    loop{
                        if let Ok(mutex) = clone.lock() {
                            if let Ok(work) = mutex.recv() {
                                after_execute(work);
                            }
                        }
                    }
                });
            });
    }

    fn update_job(&mut self, data: &'a (dyn IExecutionResult<()> + 'a)){          
        self.execution_result = Some(data);
    }

    pub fn trigger(&mut self){
        let _ = other_thread::scope(|scope|{
            let callback = Arc::new(&self.callback);
            let _ = scope.spawn(move |_| {
                let _callback = callback.clone();
                //callback.deref();
            });
        });
    }

    pub fn next_run(&self) -> DateTime<Local>{
        ChronExpression::parse(self.cron_expression.clone()).next()
    }
}

impl JobScheduler<'_>{
    pub fn new() -> Self{
        Self{
            _timers: Dict::<Arc<Mutex<JobTimer>>>::new(),
            _handles: vec![]
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

    fn private_schedule<F: FnMut() + Send + Sync + 'static + Copy,
                        FA: FnMut(&Callback) + Send + Sync + 'static + Copy>(&mut self,
                         name: &'static str,
                         cron_expression: &'static str,
                         function: F,
                         callback: Option<FA>) -> Result<bool, String>{
        if self._timers.contains_key(name) {
            return Err(format!("There's already a job named: {name}!", name = name));
        }

        let handler = thread::spawn(move || {
            JobTimer::schedule(cron_expression, function, &mut move |data: ExecutionResultParam|{
                match callback {
                    |None => {},
                    |Some(mut func) => {
                        let values: &Callback = &*data;
                        func(values);
                    }
                };
            });
        });
        
        self._timers.add(String::from(name), Arc::new(Mutex::new(JobTimer::new(cron_expression, Box::new(function)))));
        self._handles.push(handler);
        
        Ok(true)
    }

    pub fn schedule<J: Job, FA: FnMut(&Callback) + Send + Sync + 'static + Copy>(&mut self, cron_expression: &'static str) -> Result<bool, String>{
        let function = |_: &Callback|{};
        self.private_schedule(type_name::<J>(), cron_expression, || J::execute(), Some(function))
    }

    pub fn schedule_with_callback<F: FnMut() + Send + Sync + 'static + std::marker::Copy,
         FA: FnMut(&Callback) + Send + Sync + 'static + Copy>(&mut self,
        name: &'static str,
        cron_expression: &'static str,
        function: F,
        callback: Option<FA>) -> Result<bool, String>{
        self.private_schedule(name, cron_expression, function, callback)
    }

    pub fn execute<F: FnOnce() + Send + Sync + 'static + std::marker::Copy>(&mut self, function: F){
        thread::spawn(move || {
            function();
        });
    }

    pub fn trigger<J: Job>(&mut self){
        let name = type_name::<J>();

        if self._timers.contains_key(name) {
            for elem in self._timers.iter_mut() {
                if elem.key == String::from(name) {
                    let mut value = elem.val.lock().unwrap();
                    value.trigger();
                }
            }
        }
    }
}

impl ScheduleManager{
    pub fn instance() -> JobScheduler<'static> {
        JobScheduler::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::Callback;
    use crate::ChronExpression;
    use crate::ScheduleManager;
    use chrono::{DateTime, Local, Timelike, Datelike};

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

        let callback = |data: &Callback|{
            println!("{}", data.get_start_time());
            println!("{}", match data.get_run_successfuly(){ Some(x) => x, None => false});
        };

        let _job1 = scheduler.schedule_with_callback("test", "*/1 * * * *", move || { }, Some(callback));
        let _job2 = scheduler.execute(move || {
            assert_eq!(test.increment(), 1);
        });
    }
}
