import datetime
from logging import getLogger

from rm.services.letf.sabr_calibrator import SABRCalibratorMixin, BergomiCalibrationMixin, BergomiCalibrationNN
from rm.services.letf.yf_option_chain_fetcher import YFOptionChainFetcher
from rm.services.letf.yf_stock_fetcher import YFStockFetcher


_logger = getLogger(__name__)


def calibrate_sabr_one(
        ticker: str,
        expiry: datetime.date,
        calls_puts='call',
        today=datetime.date.today(),
        engine_type='sabr'  # 'sabr', 'bergomi_analytic', 'bergomi_nn'
):
    sf = YFStockFetcher(tickers=[ticker], kafka_bootstrap=None)
    F = sf.fetch_current_prices()[ticker]
    _logger.info(f"Using price {F}")
    
    if engine_type == 'bergomi_nn':
        sabr_engine = BergomiCalibrationNN()
        # Optionally pre-train or load model here if needed
        # sabr_engine.train_model(n_samples=2000, epochs=50) 
    elif engine_type == 'bergomi_analytic':
        sabr_engine = BergomiCalibrationMixin()
    else:
        sabr_engine = SABRCalibratorMixin()

    ocf = YFOptionChainFetcher(ticker, kafka_bootstrap=None)

    option_chain = ocf.fetch_option_chain(ticker, expiry)
    options = list(filter(lambda x: x['call_put'] == calls_puts, option_chain))
    ttm = (expiry - today).days/252.

    sabr_engine.calibrate(
        options,
        F=F,
        T=ttm,
    )
    F_min = sabr_engine.F/2
    F_max = sabr_engine.F*2

    F_model, vols_model = sabr_engine.plot_vols(F_max=F_max, F_min=F_min)
    F_market_model = [
        (x['strike'], x['volatility']) 
        for x in options if F_max >= x['strike'] >= F_min and x['volatility']> 0.01
    ]
    if F_market_model:
        F_market, vols_market = zip(*F_market_model)
    else:
        F_market, vols_market = [], []

    return (F_model, vols_model), (F_market, vols_market), sabr_engine


# Example usage:
# 1. Standard SABR
# model, market, sabr = calibrate_sabr_one('NVDA', datetime.date(2026, 4, 17), engine_type='sabr')

# 2. Analytic Bergomi (Bayer-Friz-Gatheral)
# model_b, market_b, bergomi = calibrate_sabr_one('NVDA', datetime.date(2026, 4, 17), engine_type='bergomi_analytic')

# 3. Neural Network Bergomi (Monte Carlo trained)
# Note: This will trigger on-the-fly training if no model is loaded, which takes time.
# model_nn, market_nn, bergomi_nn = calibrate_sabr_one('NVDA', datetime.date(2026, 4, 17), engine_type='bergomi_nn')

