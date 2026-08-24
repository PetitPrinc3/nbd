use std::time::Duration;

#[allow(dead_code)]
pub fn percentile(latencies: &mut Vec<Duration>, p: f64) -> Duration {
    if latencies.is_empty() {
        return Duration::from_secs(0);
    }
    let nb_latencies: f64 = (latencies.len() - 1) as f64;
    let target_index: u64 = ((nb_latencies * p) / 100.0) as u64;
    return latencies[target_index as usize];
}

#[allow(dead_code)]
pub fn show_report(
    test_name: &str,
    mut latencies: Vec<Duration>,
    nb_sent: Option<u64>,
    nb_received: Option<u64>,
) {
    if latencies.is_empty() {
        println!("Empty latencies !");
    } else {
        latencies.sort();
        let p50 = percentile(&mut latencies, 50.0);
        let p95 = percentile(&mut latencies, 95.0);
        let p99 = percentile(&mut latencies, 99.0);
        let p999 = percentile(&mut latencies, 99.9);
        let max = latencies[latencies.len() - 1];

        println!("╔══════════════════════════════════╗");
        println!("║ REPORT : {:<23} ║", test_name);
        println!("╠══════════════════════════════════╣");
        if nb_sent.is_some() && nb_received.is_some() {
            println!(
                "║ Accuracy            : {:>9.2}% ║",
                ((nb_received.unwrap_or_else(|| { 0 }) as f64)
                    / (nb_sent.unwrap_or_else(|| { 1 }) as f64))
                    * 100.0
            );
        };
        println!("║ Samples             : {:>10} ║", latencies.len());
        println!("║ Median (p50)        : {:>10.3?} ║", p50);
        println!("║ p95                 : {:>10.3?} ║", p95);
        println!("║ p99 (target < 1ms)  : {:>10.3?} ║", p99);
        println!("║ p99.9               : {:>10.3?} ║", p999);
        println!("║ Maximum             : {:>10.3?} ║", max);
        println!("╚══════════════════════════════════╝");
    }
}

#[cfg(windows)]
#[link(name = "ntdll")]
unsafe extern "system" {
    pub fn NtSetTimerResolution(
        DesiredResolution: u32,
        SetResolution: u8,
        ActualResolution: *mut u32,
    ) -> i32; // NTSTATUS (0 = STATUS_SUCCESS)
}
