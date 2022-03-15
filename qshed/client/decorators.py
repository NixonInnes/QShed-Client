def typed_response(response_model=None):
    def wrapper(func):
        def inner(*args, **kwargs):
            resp = func(*args, **kwargs)
            print(resp)
            resp = response_model(**resp)
            print(resp.ok, resp.message) # TODO: Add logging
            return resp.content_
        return inner
    return wrapper