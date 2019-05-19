# neural network model for trading.

import numpy as np

from keras.models import Sequential
from keras.layers import Dense, Activation


def trading_model():
    '''
    Model for stock trading.

    '''

    model =  Sequential([Dense(128, activation='relu', input_shape=(2,))
                         , Dense(2, activation='softmax' ) ])

    model.compile(optimizer = 'rmsprop'
                  , loss    = 'sparse_categorical_crossentropy'
                  , metrics = ['accuracy'] )

    return model


def get_input_data():
    '''
    Prepares the data for the model.
    '''

    stock_price_increments   = np.random.normal(size=1000)
    # stock_price_increments = - np.ones(1000)

    stock_price_history      = np.cumsum(stock_price_increments)

    # stocks level, stock increment
    input_stocks = np.vstack( (stock_price_increments[1:]
                               , stock_price_history[1:]) ).transpose()
    trading_direction = stock_price_increments[1:] > 0

    return input_stocks, trading_direction


def get_predict_data():

    stock_price_increments   = np.random.normal(size=1000)
    # stock_price_increments = - np.ones(1000)

    stock_price_history      = np.cumsum(stock_price_increments)

    # stocks level, stock increment
    input_stocks = np.vstack( (stock_price_increments[1:]
                               , stock_price_history[1:]) ).transpose()
    trading_direction = stock_price_increments[1:] > 0

    return input_stocks, trading_direction



model = trading_model()
print(model.summary())

# Train the model, iterating on the data in batches of 32 samples
input_stocks, trading_direction = get_input_data()
model.fit(input_stocks, trading_direction, epochs=10, verbose=0)

# prediction part
predict_stocks, predict_direction = get_predict_data()
predict = model.predict(predict_stocks)
predict_score = model.evaluate(predict_stocks, predict_direction)

print(predict_score)
# print(predict)
