def singleton(cls):
    """decorator for a class to make a singleton out of it"""
    instances = {}

    def get_instance(*args, **kwargs):
        """creating or just return the one and only class instance.
        The singleton depends on the parameters used in __init__"""

        from deche.inspection import args_kwargs_to_kwargs

        full_kwargs = args_kwargs_to_kwargs(cls, args, kwargs)
        key = (cls, full_kwargs)
        if key not in instances:
            instances[key] = cls(*args, **kwargs)
        return instances[key]

    return get_instance
