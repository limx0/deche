import inspect


def get_func_signature(f):
    spec = inspect.getfullargspec(f)
    assert not spec.varargs
    return inspect.signature(f)


def args_kwargs_to_kwargs(func, args, kwargs):
    sig = get_func_signature(func)
    bound_args = sig.bind(*args, **kwargs)
    bound_args.apply_defaults()
    all_kwargs = bound_args.arguments
    assert len(all_kwargs.pop("args", [])) == 0
    all_kwargs.update(all_kwargs.pop("kwargs", {}))
    return all_kwargs
