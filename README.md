# Large scale regression with `dbreg`

Playing around with [`dbreg`](https://github.com/grantmcdermott/dbreg).

Install `uv` and run:

```bash
uv sync
uv run main.py
```

Then run `regz.rmd` in Rstudio. The speed of `dbreg` is kind of crazy - interacted vars/FEs a little complicated to deal with, though. You have to replace the reference level (for instance in event studies w.r.t t=-1) to some arbitrary low integer value, such that the algo picks up the lowest value as the base/ref level. 