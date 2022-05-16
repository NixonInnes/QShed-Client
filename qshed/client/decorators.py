
def typed_response(response_model=None):
    def wrapper(func):
        def inner(*args, **kwargs):
            resp = func(*args, **kwargs)
            resp = response_model.parse_raw(resp)
            print(resp.ok, resp.message)  # TODO: Add logging
            return resp.content_

        return inner

    return wrapper
