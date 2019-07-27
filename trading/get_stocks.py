# get stocks from intrinio

# sandbox api key
sandbox_key = 'OmE2YTYwNTdmY2YyMWVhNTcwMTZkYWM0MmJkNzBkNDg3'
# production key
prod_key    = 'OjYzMmFlYTU4ZWZjY2U0OGRhZDZjYWQ3NzI0NGRhY2Qw'

import intrinio_sdk

intrinio_sdk.ApiClient().configuration.api_key['api_key'] = sandbox_key

security_api = intrinio_sdk.SecurityApi()
company_api  = intrinio_sdk.CompanyApi()

def get_security(security):

    start_date = '2019-01-02'
    end_date   = '2019-01-04'
    api_response = security_api.get_security_intraday_prices(security
                                                             , start_date = start_date
                                                             , end_date   = end_date)

    return [x.last_price for x in api_response.intraday_prices]  # prices


prices = get_security('AAPL')

def get_company_info(identifier):

    return company_api.get_company(identifier)


def get_options(symbol):

    # options part
    options_api = intrinio_sdk.OptionsApi()
    option_type = 'put'
    strike = 170.0
    strike_greater_than = 190.0
    strike_less_than = 150.0
    expiration = '2019-03-01'
    expiration_after = '2019-01-01'
    expiration_before = '2019-12-31'
    page_size = 100
    next_page = ''

    return options_api.get_options(symbol
                                   , type=option_type
                                   , strike=strike
                                   , strike_greater_than=strike_greater_than
                                   , strike_less_than=strike_less_than
                                   , expiration=expiration
                                   , expiration_after=expiration_after
                                   , expiration_before=expiration_before
                                   , page_size=page_size
                                   , next_page=next_page )
