import const
from flask import Flask


app = Flask(__name__)


@app.route('/stop', methods=['POST'])
def stop():
    f = open(const.STOP_MARKER_FILE, "a")
    f.close()
    return "stop marker file created"
