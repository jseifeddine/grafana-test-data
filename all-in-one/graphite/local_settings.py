# Graphite-web local settings for the all-in-one container.
# This file is copied into the graphite Python package directory at build time.

SECRET_KEY = 'grafana-aio-graphite-secret-not-for-production'

DATABASES = {
    'default': {
        'NAME':     '/opt/graphite/storage/graphite.db',
        'ENGINE':   'django.db.backends.sqlite3',
        'USER':     '',
        'PASSWORD': '',
        'HOST':     '',
        'PORT':     '',
    }
}

WHISPER_DIR = '/opt/graphite/storage/whisper'
LOG_DIR     = '/opt/graphite/storage/log/webapp'
TIME_ZONE   = 'UTC'

# Permit Grafana (and any other proxy) to talk to graphite-web
ALLOWED_HOSTS = ['*']
