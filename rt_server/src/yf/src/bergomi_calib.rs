use std::f64::consts::PI;

/// Rough Bergomi model + VIX option pricing + calibration.
///
/// This module is self-contained (no external crates) and implements:
/// - Rough Bergomi variance process using a hybrid scheme approximation
/// - Correlated asset/variance Brownian drivers
/// - VIX option pricing via Monte Carlo
/// - Simple Nelder–Mead calibration to market option prices
///
/// Assumptions / educated guesses:
/// - Underlying is equity index with S0=1.0 (only log-returns matter for VIX options here)
/// - Rates/dividends are ignored by default (r=0), but can be set in pricer
/// - VIX at time T is approximated as:
///     VIX_T = 100 * sqrt( (1/τ) * ∫_{T}^{T+τ} E[v_u | F_T] du )
///   and we approximate conditional expectation by using the simulated forward variance path
///   and averaging v over [T, T+τ] on each path.
/// - Option payoff is on VIX points: max(±(VIX_T - K), 0)
///
/// References (conceptual):
/// - Bayer, Friz, Gatheral: Pricing under rough volatility
/// - Gatheral, Jaisson, Rosenbaum: Volatility is rough
///
/// This is intended as a practical starting point; production use should add:
/// - variance reduction, better discretization, robust implied vol conversion, etc.

#[derive(Debug, Clone, Copy)]
pub struct BergomiModel {
    /// Hurst exponent in (0, 0.5)
    pub h: f64,
    /// Vol-of-vol (eta > 0)
    pub eta: f64,
    /// Correlation between asset and variance Brownian motions in (-1, 1)
    pub rho: f64,
    /// Initial forward variance level (xi > 0). Here treated as flat forward variance curve.
    pub xi: f64,
}

impl BergomiModel {
    pub fn new(h: f64, eta: f64, rho: f64, xi: f64) -> Self {
        Self { h, eta, rho, xi }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !(self.h > 0.0 && self.h < 0.5) {
            return Err("H must be in (0, 0.5)".to_string());
        }
        if !(self.eta > 0.0) {
            return Err("eta must be > 0".to_string());
        }
        if !(self.xi > 0.0) {
            return Err("xi must be > 0".to_string());
        }
        if !(self.rho > -1.0 && self.rho < 1.0) {
            return Err("rho must be in (-1, 1)".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionType {
    Call,
    Put,
}

#[derive(Debug, Clone)]
pub struct VixOptionQuote {
    /// Option maturity in years
    pub maturity: f64,
    /// Strike in VIX points (e.g., 18.0)
    pub strike: f64,
    pub option_type: OptionType,
    /// Market mid price in VIX points
    pub mid: f64,
    /// Optional weight (e.g., inverse bid-ask spread). Default 1.0 if None.
    pub weight: f64,
}

impl VixOptionQuote {
    pub fn new(maturity: f64, strike: f64, option_type: OptionType, mid: f64) -> Self {
        Self {
            maturity,
            strike,
            option_type,
            mid,
            weight: 1.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PricingConfig {
    /// Number of Monte Carlo paths
    pub n_paths: usize,
    /// Time steps per year (e.g., 365 for daily)
    pub steps_per_year: usize,
    /// Risk-free rate for discounting (annualized, continuously compounded)
    pub r: f64,
    /// VIX window length in years (30/365 by default)
    pub vix_window: f64,
    /// RNG seed
    pub seed: u64,
}

impl Default for PricingConfig {
    fn default() -> Self {
        Self {
            n_paths: 50_000,
            steps_per_year: 365,
            r: 0.0,
            vix_window: 30.0 / 365.0,
            seed: 42,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CalibrationConfig {
    pub pricing: PricingConfig,
    /// Max Nelder–Mead iterations
    pub max_iter: usize,
    /// Stop when simplex size is below this
    pub tol: f64,
    /// Parameter bounds: (min, max) for (h, eta, rho, xi)
    pub bounds: [(f64, f64); 4],
}

impl Default for CalibrationConfig {
    fn default() -> Self {
        Self {
            pricing: PricingConfig::default(),
            max_iter: 80,
            tol: 1e-3,
            bounds: [
                (0.02, 0.49),  // h
                (0.05, 5.0),   // eta
                (-0.999, 0.0), // rho (often negative for equities)
                (1e-4, 0.5),   // xi (variance level)
            ],
        }
    }
}

/// Public entry point: calibrate rough Bergomi to VIX option quotes.
///
/// Returns calibrated model.
pub fn calibrate_bergomi_to_vix_options(quotes: Vec<VixOptionQuote>) -> Result<BergomiModel, String> {
    let cfg = CalibrationConfig::default();
    calibrate_bergomi_to_vix_options_with_config(quotes, cfg)
}

pub fn calibrate_bergomi_to_vix_options_with_config(
    quotes: Vec<VixOptionQuote>,
    cfg: CalibrationConfig,
) -> Result<BergomiModel, String> {
    if quotes.is_empty() {
        return Err("No quotes provided".to_string());
    }
    for q in &quotes {
        if q.maturity <= 0.0 {
            return Err("Quote maturity must be > 0".to_string());
        }
        if q.strike <= 0.0 {
            return Err("Quote strike must be > 0".to_string());
        }
        if q.mid < 0.0 {
            return Err("Quote mid must be >= 0".to_string());
        }
        if q.weight <= 0.0 {
            return Err("Quote weight must be > 0".to_string());
        }
    }

    // Initial guess (educated)
    let x0 = [0.10, 1.0, -0.7, 0.04];

    let objective = |x: &[f64; 4]| -> f64 {
        let x_clamped = clamp_params(*x, &cfg.bounds);
        let model = BergomiModel::new(x_clamped[0], x_clamped[1], x_clamped[2], x_clamped[3]);
        if model.validate().is_err() {
            return 1e12;
        }
        let mut err2 = 0.0;
        for q in &quotes {
            let price = price_vix_option_mc(&model, q, &cfg.pricing);
            let diff = price - q.mid;
            err2 += q.weight * diff * diff;
        }
        err2
    };

    let (best, _best_val) = nelder_mead_4d(x0, objective, &cfg.bounds, cfg.max_iter, cfg.tol);
    let best = clamp_params(best, &cfg.bounds);
    let model = BergomiModel::new(best[0], best[1], best[2], best[3]);
    model.validate()?;
    Ok(model)
}

/// Price a single VIX option quote under rough Bergomi using Monte Carlo.
pub fn price_vix_option_mc(model: &BergomiModel, quote: &VixOptionQuote, cfg: &PricingConfig) -> f64 {
    // We simulate up to T + tau
    let t0 = 0.0;
    let t1 = quote.maturity;
    let t2 = quote.maturity + cfg.vix_window;

    let dt = 1.0 / (cfg.steps_per_year as f64);
    let n_steps = ((t2 - t0) / dt).ceil() as usize;
    let t2_eff = n_steps as f64 * dt;

    // Indices for averaging window [T, T+tau]
    let idx_t = ((t1 / dt).round() as isize).max(0) as usize;
    let idx_t2 = ((t2 / dt).round() as isize).max(0) as usize;
    let idx_t2 = idx_t2.min(n_steps);

    // Precompute kernel weights for hybrid scheme
    let kernel = HybridKernel::new(model.h, dt, n_steps);

    let mut rng = XorShift64::new(cfg.seed ^ (hash_quote(quote) as u64));

    let mut payoff_sum = 0.0;
    let disc = (-cfg.r * quote.maturity).exp();

    for _ in 0..cfg.n_paths {
        // Generate correlated Brownian increments
        // dW1 for variance driver, dW2 independent, dB = rho dW1 + sqrt(1-rho^2) dW2 for asset
        let mut d_w1 = vec![0.0f64; n_steps];
        let mut d_w2 = vec![0.0f64; n_steps];
        for i in 0..n_steps {
            d_w1[i] = rng.standard_normal() * dt.sqrt();
            d_w2[i] = rng.standard_normal() * dt.sqrt();
        }

        // Build fractional Gaussian process Y_t (hybrid approximation)
        // Y_i approximates ∫_0^{t_i} K(t_i - s) dW1_s
        let y = kernel.build_y(&d_w1);

        // Variance path v_t = xi * exp( eta * Y_t - 0.5 * eta^2 * t^{2H} )
        // Here Var(Y_t) = t^{2H} for the Riemann-Liouville fBm integral with kernel t^{H-1/2}.
        let mut v = vec![0.0f64; n_steps + 1];
        v[0] = model.xi;
        for i in 1..=n_steps {
            let t = (i as f64) * dt;
            let var_y = t.powf(2.0 * model.h);
            v[i] = model.xi * (model.eta * y[i - 1] - 0.5 * model.eta * model.eta * var_y).exp();
        }

        // Asset log-price path (not directly needed for VIX payoff, but included for completeness)
        // dX = -0.5 v dt + sqrt(v) dB
        let mut x = 0.0f64;
        let sqrt_1mr2 = (1.0 - model.rho * model.rho).max(0.0).sqrt();
        for i in 0..n_steps {
            let d_b = model.rho * d_w1[i] + sqrt_1mr2 * d_w2[i];
            let vi = v[i].max(0.0);
            x += -0.5 * vi * dt + vi.sqrt() * d_b;
        }
        let _s_t2 = x.exp(); // S0=1.0

        // Approximate VIX at T as sqrt( average variance over [T, T+tau] ) * 100
        // Use discrete average of v over indices [idx_t, idx_t2)
        let mut avg_v = 0.0;
        let mut count = 0usize;
        let start = idx_t.min(n_steps);
        let end = idx_t2.max(start + 1).min(n_steps);
        for i in start..end {
            avg_v += v[i];
            count += 1;
        }
        avg_v /= count as f64;
        let vix_t = 100.0 * avg_v.max(0.0).sqrt();

        let payoff = match quote.option_type {
            OptionType::Call => (vix_t - quote.strike).max(0.0),
            OptionType::Put => (quote.strike - vix_t).max(0.0),
        };

        payoff_sum += payoff;
    }

    disc * (payoff_sum / (cfg.n_paths as f64))
}

/// --- Hybrid kernel approximation for rough Bergomi ---
///
/// We approximate:
///   Y(t_i) = ∫_0^{t_i} (t_i - s)^{H-1/2} dW_s
///
/// Using a simple hybrid scheme:
/// - For the most recent interval, use exact integral weight
/// - For older intervals, use power-law weights on Brownian increments
///
/// This is a pragmatic implementation; for higher accuracy, consider:
/// - exact covariance-based sampling of fGn
/// - improved hybrid discretization
struct HybridKernel {
    h: f64,
    dt: f64,
    n: usize,
    // weights w[k] for contribution of dW[i-k] to Y[i]
    // i.e., Y[i] = sum_{k=1..=i} w[k] * dW[i-k]
    w: Vec<f64>,
}

impl HybridKernel {
    fn new(h: f64, dt: f64, n: usize) -> Self {
        // Precompute weights for k=1..=n:
        // w_k = ∫_{(k-1)dt}^{k dt} u^{H-1/2} du = ( (k dt)^{H+1/2} - ((k-1)dt)^{H+1/2} ) / (H+1/2)
        let hp = h + 0.5;
        let mut w = vec![0.0f64; n + 1];
        for k in 1..=n {
            let a = (k as f64 * dt).powf(hp);
            let b = ((k - 1) as f64 * dt).powf(hp);
            w[k] = (a - b) / hp;
        }
        Self { h, dt, n, w }
    }

    fn build_y(&self, d_w: &[f64]) -> Vec<f64> {
        // d_w length = n
        // y length = n, y[i] corresponds to time t_{i+1} in variance formula usage
        let n = self.n;
        let mut y = vec![0.0f64; n];
        // O(n^2) convolution; acceptable for moderate n. For production, use FFT.
        for i in 0..n {
            let mut acc = 0.0;
            // k=1..=i+1
            for k in 1..=i + 1 {
                // contribution from dW[i+1-k]
                acc += self.w[k] * d_w[i + 1 - k];
            }
            y[i] = acc;
        }
        y
    }
}

/// --- Nelder–Mead optimizer in 4D (simple, dependency-free) ---
fn nelder_mead_4d<F>(
    x0: [f64; 4],
    f: F,
    bounds: &[(f64, f64); 4],
    max_iter: usize,
    tol: f64,
) -> ([f64; 4], f64)
where
    F: Fn(&[f64; 4]) -> f64,
{
    // Initial simplex: x0 plus small steps in each coordinate
    let mut simplex = vec![x0; 5];
    let steps = [0.02, 0.2, 0.05, 0.01];
    for i in 0..4 {
        let mut xi = x0;
        xi[i] += steps[i];
        xi = clamp_params(xi, bounds);
        simplex[i + 1] = xi;
    }

    let mut vals: Vec<f64> = simplex.iter().map(|x| f(x)).collect();

    let alpha = 1.0;
    let gamma = 2.0;
    let rho = 0.5;
    let sigma = 0.5;

    for _iter in 0..max_iter {
        // sort by vals
        let mut idx: Vec<usize> = (0..5).collect();
        idx.sort_by(|&a, &b| vals[a].partial_cmp(&vals[b]).unwrap());

        simplex = idx.iter().map(|&i| simplex[i]).collect();
        vals = idx.iter().map(|&i| vals[i]).collect();

        // check simplex size
        let size = simplex_diameter(&simplex);
        if size < tol {
            break;
        }

        let best = simplex[0];
        let worst = simplex[4];
        let second_worst_val = vals[3];

        // centroid of best 4 points
        let centroid = centroid_4(&simplex[0..4]);

        // reflection
        let mut xr = add(centroid, scale(sub(centroid, worst), alpha));
        xr = clamp_params(xr, bounds);
        let fr = f(&xr);

        if fr < vals[0] {
            // expansion
            let mut xe = add(centroid, scale(sub(xr, centroid), gamma));
            xe = clamp_params(xe, bounds);
            let fe = f(&xe);
            if fe < fr {
                simplex[4] = xe;
                vals[4] = fe;
            } else {
                simplex[4] = xr;
                vals[4] = fr;
            }
        } else if fr < second_worst_val {
            simplex[4] = xr;
            vals[4] = fr;
        } else {
            // contraction
            let mut xc;
            if fr < vals[4] {
                // outside contraction
                xc = add(centroid, scale(sub(xr, centroid), rho));
            } else {
                // inside contraction
                xc = add(centroid, scale(sub(worst, centroid), -rho));
            }
            xc = clamp_params(xc, bounds);
            let fc = f(&xc);

            if fc < vals[4] {
                simplex[4] = xc;
                vals[4] = fc;
            } else {
                // shrink
                for i in 1..5 {
                    simplex[i] = clamp_params(add(best, scale(sub(simplex[i], best), sigma)), bounds);
                    vals[i] = f(&simplex[i]);
                }
            }
        }
    }

    // return best
    let mut idx: Vec<usize> = (0..5).collect();
    idx.sort_by(|&a, &b| vals[a].partial_cmp(&vals[b]).unwrap());
    let best_i = idx[0];
    (simplex[best_i], vals[best_i])
}

fn centroid_4(points: &[[f64; 4]]) -> [f64; 4] {
    let mut c = [0.0; 4];
    for p in points {
        for i in 0..4 {
            c[i] += p[i];
        }
    }
    for i in 0..4 {
        c[i] /= points.len() as f64;
    }
    c
}

fn simplex_diameter(simplex: &[[f64; 4]]) -> f64 {
    let mut max_d = 0.0;
    for i in 0..simplex.len() {
        for j in i + 1..simplex.len() {
            let d = l2(sub(simplex[i], simplex[j]));
            if d > max_d {
                max_d = d;
            }
        }
    }
    max_d
}

fn l2(x: [f64; 4]) -> f64 {
    (x[0] * x[0] + x[1] * x[1] + x[2] * x[2] + x[3] * x[3]).sqrt()
}

fn add(a: [f64; 4], b: [f64; 4]) -> [f64; 4] {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3]]
}

fn sub(a: [f64; 4], b: [f64; 4]) -> [f64; 4] {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2], a[3] - b[3]]
}

fn scale(a: [f64; 4], s: f64) -> [f64; 4] {
    [a[0] * s, a[1] * s, a[2] * s, a[3] * s]
}

fn clamp_params(x: [f64; 4], bounds: &[(f64, f64); 4]) -> [f64; 4] {
    let mut y = x;
    for i in 0..4 {
        if y[i] < bounds[i].0 {
            y[i] = bounds[i].0;
        }
        if y[i] > bounds[i].1 {
            y[i] = bounds[i].1;
        }
    }
    y
}

/// --- Minimal RNG + Normal generator (Box-Muller) ---
struct XorShift64 {
    state: u64,
    has_spare: bool,
    spare: f64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let seed = if seed == 0 { 0xdead_beef_cafe_babe } else { seed };
        Self {
            state: seed,
            has_spare: false,
            spare: 0.0,
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_f64(&mut self) -> f64 {
        // uniform in (0,1)
        let u = self.next_u64();
        // 53 bits precision
        let v = (u >> 11) as u64;
        (v as f64 + 1.0) / ((1u64 << 53) as f64 + 2.0)
    }

    fn standard_normal(&mut self) -> f64 {
        if self.has_spare {
            self.has_spare = false;
            return self.spare;
        }
        let u1 = self.next_f64().max(1e-16);
        let u2 = self.next_f64();
        let r = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * PI * u2;
        let z0 = r * theta.cos();
        let z1 = r * theta.sin();
        self.spare = z1;
        self.has_spare = true;
        z0
    }
}

fn hash_quote(q: &VixOptionQuote) -> u64 {
    // Simple deterministic hash to decorrelate RNG streams per quote
    let mut h = 1469598103934665603u64;
    for b in q.maturity.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211u64);
    }
    for b in q.strike.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211u64);
    }
    let ot = match q.option_type {
        OptionType::Call => 1u64,
        OptionType::Put => 2u64,
    };
    h ^= ot;
    h = h.wrapping_mul(1099511628211u64);
    h
}
