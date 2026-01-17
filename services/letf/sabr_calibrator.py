import numpy as np
from typing import Dict, Any, List, Optional
from scipy.optimize import minimize, brentq
from scipy.stats import norm
from logging import getLogger
import torch
import torch.nn as nn
import torch.optim as optim

_logger = getLogger(__name__)


def _bs_price(F, K, T, vol, call_put='call'):
    d1 = (np.log(F / K) + 0.5 * vol ** 2 * T) / (vol * np.sqrt(T))
    d2 = d1 - vol * np.sqrt(T)
    if call_put == 'call':
        return F * norm.cdf(d1) - K * norm.cdf(d2)
    else:
        return K * norm.cdf(-d2) - F * norm.cdf(-d1)


def _bs_implied_vol(price, F, K, T, call_put='call'):
    def objective(vol):
        return _bs_price(F, K, T, vol, call_put) - price

    try:
        return brentq(objective, 1e-6, 5.0)
    except ValueError:
        return np.nan


class SABRCalibratorMixin:
    """
    Calibrates SABR model parameters (alpha, beta, rho, nu)
    for option chains fetched via YFOptionChainFetcher.
    """

    def __init__(self):
        self.params = None
        # self.beta = beta
        # self.alpha = None
        # self.rho = None
        # self.nu = None
        self.F = None
        self.T = None

    def initial_params(self):
        # alpha  : Initial volatility (scaling factor)
        # beta   : Elasticity parameter (0 = Normal, 1 = Lognormal)
        # rho    : Correlation between asset and volatility
        # nu     : Volatility of volatility (nu)
        # (alpha, rho, nu, beta) params of the SABR model.
        return [0.44, -0.5, 0.5, 0.5]

    def bounds_params(self):
        # bounds on the 4 params above.
        # (alpha, rho, nu, beta)
        return [(1e-4, 5.0), (-0.999, 0.999), (1e-4, 25.0), (1e-4, .99)]
        # return [(0.43, 0.45), (-0.999, 0.999), (1e-4, 25.0), (1e-4, .99)]

    def results_display(self):
        alpha, rho, nu, beta = self.params
        _logger.info(
            f'Results: alpha = {alpha}, beta = {beta}, rho = {rho}, nu = {nu}'
        )

    @staticmethod
    def model_vol(F: float, K: float, T: float, params: List[float]) -> float:
        """
        Hagan et al. (2002) SABR implied volatility approximation.
        """

        alpha, rho, nu, beta = params

        if F == K:
            term1 = (alpha / (F ** (1 - beta)))
            term2 = 1 + (((1 - beta) ** 2 / 24) * (alpha ** 2 / (F ** (2 - 2 * beta))) +
                         (rho * beta * nu * alpha / (4 * (F ** (1 - beta)))) +
                         ((2 - 3 * rho ** 2) * (nu ** 2) / 24)) * T
            return term1 * term2

        FK = F * K
        logFK = np.log(F / K)
        z = (nu / alpha) * (FK) ** ((1 - beta) / 2) * logFK
        x_z = np.log((np.sqrt(1 - 2 * rho * z + z ** 2) + z - rho) / (1 - rho))
        num = alpha * (1 + (((1 - beta) ** 2 / 24) * (np.log(F / K)) ** 2 +
                            ((1 - beta) ** 4 / 1920) * (np.log(F / K)) ** 4))
        denom = (FK) ** ((1 - beta) / 2) * (1 + ((1 - beta) ** 2 / 24) * (np.log(F / K)) ** 2)
        vol = (num / denom) * (z / x_z) * (1 + (((1 - beta) ** 2 / 24) * (alpha ** 2 / (FK ** (1 - beta))) +
                                                (rho * beta * nu * alpha / (4 * (FK ** ((1 - beta) / 2)))) +
                                                ((2 - 3 * rho ** 2) * (nu ** 2) / 24)) * T)
        return vol

    def plot_vols(self, F_min: float, F_max: float):
        x_vals = np.linspace(F_min, F_max, 40)
        y_vals = [self.model_vol(F, self.F, self.T, self.params)
                  for F in x_vals]

        return x_vals, y_vals

    def calibrate(
            self,
            option_chain: List[Dict[str, Any]],
            F: float,
            T: float,
            # beta: float = 0.5,
    ) -> Dict[str, Any]:
        """
        Calibrates SABR parameters to market implied volatilities.
        Expects option_chain with columns ['strike', 'impliedVolatility'].
        """
        strikes = [option['strike'] for option in option_chain]
        vols = [option['volatility'] for option in option_chain]

        def objective(params):
            # alpha, rho, nu, beta = params
            model_vols = [
                self.model_vol(K, F, T, params)  # alpha, beta, rho, nu)
                for K in strikes
            ]

            return np.mean(
                [(model_vol - vol)**2
                 for (model_vol, vol) in zip(model_vols, vols)
                 ]
            )

        initial_guess = self.initial_params()
        bounds = self.bounds_params()
        result = minimize(
            objective,
            initial_guess,
            bounds=bounds,
            #             method='Nelder-Mead',
            method="L-BFGS-B",
        )

        self.params = result.x
        self.F = F
        self.T = T

        _logger.info(
            f"Calibration results: Success: {result.success}, Message: {result.message}. Results: {self.results_display()}"
        )

        return {
            "params": self.params,
            # "Alpha": self.alpha,
            # "Beta": self.beta,
            # "Rho": self.rho,
            # "Nu": self.nu,
            "success": result.success,
            "message": result.message,
        }


class BergomiCalibrationMixin(SABRCalibratorMixin):

    def initial_params(self):
        # (alpha, H, rho, eta) params of the rough Bergomi model.
        return [0.2, 0.25, 0.5, 0.5]

    def bounds_params(self):
        # bounds on the 4 params above.
        return [(-np.inf, np.inf), (0.01, 0.5), (-0.999, 0.999), (0.01, np.inf)]

    @staticmethod
    def model_vol(F: float, K: float, T: float, params: List[float]) -> float:
        """
        Rough Bergomi model implied volatility.
        Uses the Bayer-Friz-Gatheral (2015) short-maturity asymptotic approximation
        for the at-the-money skew and curvature, extrapolated to a volatility smile.

        params: [alpha, beta, rho, nu]
          alpha: initial volatility (sqrt(xi))
          beta: Hurst exponent H
          rho: correlation
          nu: vol of vol (eta)
        """
        alpha, beta, rho, nu = params
        xi = alpha ** 2
        H = beta
        eta = nu

        if not (0.01 < H < 0.5): return 1e-6
        if xi <= 0: return 1e-6
        if eta < 0: return 1e-6
        if not (-0.999 < rho < 0.999): return 1e-6

        # Log-moneyness
        k = np.log(K / F)

        # Bayer-Friz-Gatheral (2015) "Pricing under rough volatility"
        # Asymptotic implied volatility structure:
        # sigma_BS(k, T) approx sigma_0 * (1 + skew * k + curv * k^2 / 2)

        # term structure of variance (flat xi assumption)
        # In rBergomi, variance process is v_t = xi * exp(eta * W_t^H - ...)
        # ATM volatility is approx sqrt(xi)
        sigma_0 = np.sqrt(xi)

        # Skew: C_H * rho * eta / (sigma_0 * T^(0.5-H))
        # C_H = sqrt(2H) / (H + 1/2)  <-- approximate constant from fractional integral
        # Actually, BFG 2015 derive the skew explicitly as:
        # Skew(T) ~ rho * eta * C(H) / T^(0.5-H)
        # where C(H) involves Gamma functions.
        # Simplified here:
        # For H < 0.5, term T^(H-0.5) explodes as T->0, reproducing the power-law skew.

        # Using the formula from Gatheral's "Volatility is Rough" slides/papers:
        # Skew ~ rho * eta * Gamma(H + 0.5) / (2 * Gamma(H + 1.5)) * T^(H - 0.5)
        # Note: Gamma(x+1) = x*Gamma(x)
        # C(H) approx 1/2 for small H

        # Using a tractable approximation for the skew term:
        # skew_term = (rho * eta) / (2 * sigma_0) * T**(H - 0.5) 
        # But let's use the explicit one if possible or a robust approximation.

        # The BFG formula for ATM skew psi(T):
        # psi(T) = rho * eta / 2 * T^(H-0.5) * (constant depending on H)
        # We'll use the simplified version often cited:
        skew = (rho * eta / (2 * sigma_0)) * (T ** (H - 0.5))

        # Curvature (convexity):
        # curv = eta^2 / (4 * sigma_0^2) * (1 - rho^2) * T^(2H - 1) * (constant)
        # This allows for the "smile" shape.
        curv = (eta ** 2 * (1 - rho ** 2)) / (4 * xi) * (T ** (2 * H - 1))

        # Asymptotic Implied Volatility
        # sigma(k) = sigma_0 * (1 + skew * k + 0.5 * curv * k^2)
        # Note: This is a Taylor expansion around k=0 (ATM).
        # For large k, this polynomial explodes, so we dampen it or trust it only for near-money.

        vol_approx = sigma_0 * (1 + skew * k + 0.5 * curv * k ** 2)

        if vol_approx <= 0:
            return 1e-6

        return vol_approx


class BergomiNet(nn.Module):
    def __init__(self, input_dim=6, hidden_dim=64):
        super(BergomiNet, self).__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1),
            nn.Softplus()  # Ensure positive volatility
        )

    def forward(self, x):
        return self.net(x)


class BergomiCalibrationNN(SABRCalibratorMixin):
    def __init__(self, model_path: Optional[str] = None):
        super().__init__()
        self.device = "cpu"  # torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = BergomiNet(input_dim=6).to(self.device)
        self.model_trained = False

        if model_path:
            try:
                self.model.load_state_dict(torch.load(model_path, map_location=self.device))
                self.model.eval()
                self.model_trained = True
                _logger.info(f"Loaded BergomiNN from {model_path}")
            except Exception as e:
                _logger.warning(f"Could not load model from {model_path}: {e}")

    def initial_params(self):
        # (alpha, H, rho, eta)
        # alpha = initial vol
        # H ... hurst parameter 0 < H < 0.5
        # rho ... correlation between vol of vol and stock
        # eta .. initial vol of vol.
        return [0.2, 0.25, -0.7, 1.5]

    def bounds_params(self):
        # (alpha, H, rho, eta), see meanings above.
        return [(1e-4, 5.0), (0.01, 0.49), (-0.999, 0.999), (0.01, 5.0)]

    def results_display(self):
        alpha, H, rho, eta = self.params
        _logger.info(
            f'Results: alpha = {alpha}, H = {H}, rho = {rho}, eta = {eta}'
        )

    def _generate_mc_samples(self, n_samples: int = 1000, T_max: float = 2.0):
        """
        Generate random (parameters, T, k) -> implied_vol samples
        using the Monte Carlo pricer for training data.
        """
        rng = np.random.default_rng()

        # Sample parameters uniformly within reasonable bounds
        alpha_bounds, H_bounds, rho_bounds, eta_bounds = self.bounds_params()

        alphas = rng.uniform(alpha_bounds[0], alpha_bounds[1], n_samples)
        Hs = rng.uniform(H_bounds[0], H_bounds[1], n_samples)
        rhos = rng.uniform(rho_bounds[0], rho_bounds[1], n_samples) # Equities typically have negative skew
        etas = rng.uniform(eta_bounds[0], eta_bounds[1], n_samples)

        Ts = rng.uniform(0.1, T_max, n_samples)
        ks = rng.uniform(-0.9, 0.9, n_samples) # log-moneyness

        X_data = []
        y_data = []

        # We need to run MC for each sample. This is slow, so we do it in a loop
        # In a real scenario, this would be parallelized or pre-computed.
        # Re-using the logic from the previous MC implementation (but putting it here explicitly)

        n_steps = 100
        n_paths = 5000  # Lower paths for speed during data gen, maybe insufficient
        dW_joint = rng.standard_normal((n_paths, n_steps))

        for i in range(n_samples):
            alpha, H, rho, eta = alphas[i], Hs[i], rhos[i], etas[i]
            T, k = Ts[i], ks[i]

            # Convert log-moneyness to Strike (assuming F=1 for training)
            F = 1.0
            K = F * np.exp(k)

            # --- MC Pricer Logic ---
            xi = alpha ** 2
            dt = T / n_steps
            sqrt_dt = np.sqrt(dt)

            # Random variates
            # dW1 = rng.standard_normal((n_paths, n_steps)) * sqrt_dt
            # dW2 = rng.standard_normal((n_paths, n_steps)) * sqrt_dt
            dW1 = dW_joint * sqrt_dt
            dW2 = dW_joint * sqrt_dt

            # Volterra kernel approximation
            k_idx = np.arange(1, n_steps + 1)
            G = np.sqrt(2 * H) * (k_idx * dt) ** (H - 0.5)
            M = np.zeros((n_steps, n_steps))
            for j in range(1, n_steps):
                M[j, :j] = G[:j][::-1]
            Y = dW1 @ M.T

            # Variance process
            t_vals = np.arange(n_steps) * dt
            drift_correction = 0.5 * eta ** 2 * (t_vals ** (2 * H))
            V = xi * np.exp(eta * Y - drift_correction)

            # Price process
            dZ = rho * dW1 + np.sqrt(1 - rho ** 2) * dW2
            log_ret = -0.5 * V * dt + np.sqrt(V) * dZ
            total_log_ret = np.sum(log_ret, axis=1)
            ST = F * np.exp(total_log_ret)

            payoff = np.maximum(ST - K, 0)
            price = np.mean(payoff)

            # Implied vol
            vol = _bs_implied_vol(price, F, K, T)

            if not np.isnan(vol) and vol > 0:
                X_data.append([alpha, H, rho, eta, T, k])
                y_data.append(vol)

        return np.array(X_data, dtype=np.float32), np.array(y_data, dtype=np.float32)

    def train_model(self, n_samples=5000, epochs=100, batch_size=64):
        _logger.info(f"Generating {n_samples} MC samples for training...")
        X_np, y_np = self._generate_mc_samples(n_samples=n_samples)

        # Prepare Tensors
        # Inputs: [alpha, H, rho, eta, T, k] -> Network input dim should be 6
        # Wait, previous class defined input_dim=5. Let's adjust.
        # Let's say inputs are (alpha, H, rho, eta, T*k? No, separate T and k).
        # We need to re-instantiate model if dimensions don't match or fix model def.
        # Let's fix model def in __init__ to input_dim=6.

        X = torch.from_numpy(X_np).to(self.device)
        y = torch.from_numpy(y_np).unsqueeze(1).to(self.device)

        dataset = torch.utils.data.TensorDataset(X, y)
        loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)

        optimizer = optim.Adam(self.model.parameters(), lr=1e-3)
        criterion = nn.MSELoss()

        _logger.info("Starting training...")
        self.model.train()
        for epoch in range(epochs):
            total_loss = 0
            for batch_X, batch_y in loader:
                optimizer.zero_grad()
                pred = self.model(batch_X)
                loss = criterion(pred, batch_y)
                loss.backward()
                optimizer.step()
                total_loss += loss.item()

            if (epoch + 1) % 10 == 0:
                _logger.info(f"Epoch {epoch+1}/{epochs}, Loss: {total_loss / len(loader):.6f}")

        self.model_trained = True
        self.model.eval()
        _logger.info("Training complete.")

    def model_vol(self, F: float, K: float, T: float, params: List[float]) -> float:
        if not self.model_trained:
            # Fallback or error? For now, let's train on the fly (tiny sample) or raise
            # Better to fallback to the analytic BFG approx or warn.
            _logger.warning("BergomiNN not trained. Training on small sample now...")
            # Re-init model with correct dim
            self.model = BergomiNet(input_dim=6).to(self.device)
            self.train_model(n_samples=1000, epochs=100) # Tiny training for demo

        # Prepare input
        # params: [alpha, H, rho, eta]
        alpha, H, rho, eta = params
        k = np.log(K / F)

        # Input vector: [alpha, H, rho, eta, T, k]
        x_in = torch.tensor([[alpha, H, rho, eta, T, k]], dtype=torch.float32).to(self.device)

        with torch.no_grad():
            vol = self.model(x_in).item()

        return vol
