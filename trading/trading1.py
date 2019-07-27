# neural network model for trading.

import numpy as np

from keras.models import Sequential
from keras.layers import Dense, Activation, RNN, Layer, LSTM, TimeDistributed
from keras.preprocessing.sequence import TimeseriesGenerator


def simple_dense_trading_model():
    '''Simple dense trading model. A few dense NN layers.

    '''

    model =  Sequential([Dense(128, activation='relu', input_shape=(2,))
                         , Dense(64, activation='relu')
                         , Dense(2, activation='softmax' ) ])

    model.compile(optimizer = 'rmsprop'
                  , loss    = 'sparse_categorical_crossentropy'
                  , metrics = ['accuracy'] )

    return model


def simple_lstm_model():
    '''Define simple lstm model.

    '''

    n_feat = 2
    # input_shape = (time_history, number of time series) - 12 historical data points, 2 data series
    model =  Sequential([ LSTM(100, activation='relu', input_shape=(12,n_feat))
                        , TimeDistributed(Dense(10), input_shape=(None, 12, n_feat)) ])

    model.compile(optimizer = 'rmsprop'
                  , loss    = 'sparse_categorical_crossentropy'
                  , metrics = ['accuracy'] )

    return model


def reorder_stock_data(input_prices):
    '''
    Constructs the input for the trading model.

    :returns: trading direction
    '''

    trading_increments = np.diff(input_prices)/input_prices[:-1]
    trading_direction  = trading_increments > 0

    return np.vstack((trading_increments[:-1]
                      , np.diff(trading_increments))).transpose()\
            , trading_direction[:-1]


def reorder_stock_data2(input_prices):
    '''
    Constructs the input for the trading model.

    :returns: trading direction
    '''

    trading_increments = np.diff(input_prices)/input_prices[:-1]
    trading_direction  = trading_increments > 0

    return np.vstack((trading_increments[:-1]
                      , np.diff(trading_increments)
                      , trading_direction[:-1])).transpose()\
           , trading_direction


def fit_dense_model(input_prices):

    stock_inputs, stock_direction = reorder_stock_data(input_prices)
    model = simple_dense_trading_model()

    model.fit(stock_inputs, stock_direction, epochs=10, verbose=1)
    #predict = model.predict(predict_stocks)
    #predict_score = model.evaluate(predict_stocks, predict_direction)

    return None


def fit_lstm_model(input_prices):
    ''' Fitting a basic long-short term model.

    '''
    model = simple_lstm_model()
    from get_stocks import prices
    stocks, direction = reorder_stock_data(prices)
    generator = TimeseriesGenerator(stocks, direction, length=12)

    model.fit_generator(generator, epochs=10, verbose=1)
    predict_direction = model.predict(stocks)
    predict_score = model.evaluate(stocks, predict_direction)
    return predict_score


# prediction part
# predict_stocks, predict_direction = get_predict_data()
# 10 is the length of the output sequence
# sample_data = TimeseriesGenerator(predict_stocks, predict_direction, 10)
from get_stocks import prices
#stocks, direction = reorder_stock_data(prices)
#generator = TimeseriesGenetor(stocks, direction, length=2)


#fit_dense_model(prices)
# def __main__():
fit_lstm_model(prices)
