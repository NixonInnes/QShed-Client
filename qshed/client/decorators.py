
def typed_response(response_model=None):
    def wrapper(func):
        def inner(*args, **kwargs):
            rtn = func(*args, **kwargs)
            print(rtn)
            resp = response_model.parse_raw(rtn)
            if resp.ok:
                return resp.content_
            return None

        return inner

    return wrapper
