# Testing

On GitHub: Actions → Tests and demo → Run workflow → main → Run workflow. Read the test and sample-scenario logs in each job.

Locally: `python -m unittest discover -s tests -v`, then `python demo.py`.

The demo is synthetic and uses an offline subscriber adapter. Local HTTP ingestion and redirect rejection are tested automatically; external subscriber integration is not claimed.
