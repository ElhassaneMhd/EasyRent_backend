import multiprocessing

# Server socket
bind = "127.0.0.1:5001"
backlog = 2048

# Worker processes
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = 'sync'
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50
timeout = 300
graceful_timeout = 30
keepalive = 2

# Restart workers after this many requests, to help limit memory leaks
max_requests_per_child = 1000

# Logging
accesslog = '/var/log/easyrent/access.log'
errorlog = '/var/log/easyrent/error.log'
loglevel = 'info'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = 'easyrent-backend'

# Server mechanics
daemon = False
pidfile = '/var/run/easyrent.pid'
tmp_upload_dir = '/tmp'

# SSL (optional, if you want to handle SSL at app level instead of nginx)
# keyfile = '/path/to/keyfile'
# certfile = '/path/to/certfile'