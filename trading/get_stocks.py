# get stocks from intrinio

# sandbox api key
api_key = 'OmE2YTYwNTdmY2YyMWVhNTcwMTZkYWM0MmJkNzBkNDg3'

import intrinio_sdk

intrinio_sdk.ApiClient().configuration.api_key['api_key'] = api_key

security_api = intrinio_sdk.SecurityApi()

identifier = 'AAPL'
start_date = '2019-01-02'
end_date   = '2019-01-04'

api_response = security_api.get_security_intraday_prices(identifier
                                                         , start_date = start_date
                                                         , end_date   = end_date)
