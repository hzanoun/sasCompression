from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/files/<file_id>/download', methods=['GET'])
def download_file(file_id):
    return "This is mock file content.", 200


@app.route('/files', methods=['POST'])
def upload_file():
    return jsonify({"file_id": "new_mock_file_id"}), 201


if __name__ == '__main__':
    app.run(port=5000)
