import numpy as np
from typing import Dict, Any, List
from scipy.optimize import minimize
from logging import getLogger

_logger = getLogger(__name__)


class SABRCalibratorMixin:
    """
    Calibrates SABR model parameters (alpha, beta, rho, nu)
    for option chains fetched via YFOptionChainFetcher.
    """

    # def __init__(self, beta: float = 0.5):
    #     self.beta = beta

    @staticmethod
    def sabr_vol(F: float, K: float, T: float, alpha: float, beta: float, rho: float, nu: float) -> float:
        """
        Hagan et al. (2002) SABR implied volatility approximation.
        """
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

    def calibrate(
            self,
            option_chain: List[Dict[str, Any]],
            F: float,
            T: float,
            beta: float = 0.5,
    ) -> Dict[str, Any]:
        """
        Calibrates SABR parameters to market implied volatilities.
        Expects option_chain with columns ['strike', 'impliedVolatility'].
        """
        strikes = [option['strike'] for option in option_chain]
        vols = [option['volatility'] for option in option_chain]

        def objective(params):
            alpha, rho, nu = params
            model_vols = [self.sabr_vol(F, K, T, alpha, beta, rho, nu) for K in strikes]

            return np.mean([(model_vol - vol)**2 for (model_vol, vol) in zip(model_vols, vols)])

        initial_guess = [0.2, 0.0, 0.5]
        bounds = [(1e-4, 5.0), (-0.999, 0.999), (1e-4, 5.0)]
        result = minimize(objective, initial_guess, bounds=bounds, method="L-BFGS-B")

        return {
            "alpha": result.x[0],
            "beta": beta,
            "rho": result.x[1],
            "nu": result.x[2],
            "success": result.success,
            "message": result.message,
        }


if __name__ == "__main__":
    from services.letf.yf_option_chain_fetcher import YFOptionChainFetcher

    fetcher = YFOptionChainFetcher(["AAPL"])
    option_chain = fetcher.fetch_option_chain("AAPL")
    expiries = fetcher._get_ticker_expiries('AAPL')
    expiry1 = expiries[0].strftime("%Y-%m-%d")


    F = np.mean([opt['lastPrice'] for opt in data])
    T = 30 / 365  # assume 30 days to expiry
        calibrator = SABRCalibratorMixin()
        params = calibrator.calibrate(calls, F, T)
        _logger.info("Calibrated SABR parameters:", params)

    else:
        _logger.error("Error fetching option chain:", data["error"])
