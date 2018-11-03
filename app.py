import json
import subprocess
import humanfriendly

from flask import Flask, Response, request, render_template
from functools import cmp_to_key
from re import findall
from werkzeug.datastructures import Headers

from sarge import run, Capture

app = Flask(__name__)


def execute_rclone(command):
    x = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE
    ).communicate()[0]
    return x


def get_rclone_return_code(command):
    child = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE
    )
    streamdata = child.communicate()[0]

    rc = child.returncode
    return rc


def is_directory(remote, path):
    # TODO: cache this result
    return get_rclone_return_code('rclone rmdir --dry-run "' + remote + path + '"') == 0


def get_data(remote, path, begin, end):
    # TODO: exponential increase/backoff
    # make this configurable
    chunk_size = 1000000
    current_pointer = begin

    while current_pointer <= end:
        if current_pointer + chunk_size > end:
            chunk_size = end - current_pointer
        elif current_pointer == end:
            chunk_size = 1

        # TODO: unsure if the math here is right
        cmd = 'rclone cat "' + remote + path + '" --offset ' + str(current_pointer) + ' --count ' + str(chunk_size)
        print(cmd)
        print(begin, end, current_pointer, chunk_size)
        current_pointer += chunk_size

        #data = execute_rclone(cmd)
        p = run(cmd, stdout=Capture(), async_=False)
        data = p.stdout.read(100000)
        while data:
            print('Read chunk: %d bytes' % (len(data)))
            yield data
            data = p.stdout.read(100000)


def get_datax(remote, path, begin, end):
    # TODO: exponential increase/backoff
    # make this configurable
    chunk_size = 1000000
    current_pointer = begin

    while current_pointer <= end:
        if current_pointer + chunk_size > end:
            chunk_size = end - current_pointer
        elif current_pointer == end:
            chunk_size = 1

        # TODO: unsure if the math here is right
        cmd = 'rclone cat "' + remote + path + '" --offset ' + str(current_pointer) + ' --count ' + str(chunk_size)
        data = execute_rclone(cmd)
        print(cmd)
        print(begin, end, current_pointer, chunk_size)
        current_pointer += chunk_size

        yield data


def sort_directory_list(a, b):
    if a['is_dir'] != b['is_dir']:
        if a['is_dir']:
            return -1
        else:
            return 1

    if a['name'] > b['name']:
        return 1
    elif a['name'] < b['name']:
        return -1

    return 0


def show_directory(remote, path):
    # TODO: put this in its own utility method
    remotes_raw = execute_rclone('rclone listremotes --long').decode('utf-8')
    remotes = []
    for r in remotes_raw.split("\n"):
        remote_tokens = r .split(' ')
        if remote_tokens[0]:
            remotes.append((remote_tokens[0].strip(), remote_tokens[-1].strip()))

    if not remote:
        return render_template('select_remote.html', remotes=remotes)

    file_list_data = execute_rclone('rclone lsf --format "psm" "' + remote + path + '"').decode('utf-8')
    file_list = []
    for item in file_list_data.split('\n'):
        if not item:
            continue
        item_tokens = item.strip().split(';')
        file_list.append({
            'name': item_tokens[0],
            'size': item_tokens[1],
            'type': item_tokens[2],
            'is_dir': item_tokens[2] == 'inode/directory',
            'human_size': humanfriendly.format_size(int(item_tokens[1]))
        })

    file_list.sort(key=cmp_to_key(sort_directory_list))

    path_tokens = path.split('/')

    path_links = []

    for i, token in enumerate(path_tokens):
        href = '/'.join(path_tokens[0:i+1])
        path_links.append((token, href))

    # TODO: show number of items or size?
    return render_template('file_manager.html', remote=remote, remotes=remotes, path_links=path_links, file_list=file_list)


def serve_file(remote, path):

    file_metadata = execute_rclone('rclone lsjson "' + remote + path + '"')
    file_json = json.loads(file_metadata)[0]

    headers = Headers()

    status = 200
    size = file_json['Size']
    mime = file_json['MimeType']
    begin = 0
    end = size - 1

    if request.headers.has_key("Range"):
        status = 206
        ranges = findall(r"\d+", request.headers["Range"])
        begin = int(ranges[0])
        if len(ranges) > 1:
            end = int(ranges[1])

        # TODO: Is the math here correct?
        headers.add('Content-Range', 'bytes %s-%s/%s' % (begin, end, size))

    headers.add('Content-Length', end-begin + 1)
    headers.add('Accept-Ranges', 'bytes')

    r = Response(
        get_data(remote, path, begin, end),
        status,
        headers=headers,
        mimetype=mime,
        direct_passthrough=True,
    )
    return r


@app.route('/slideshow', methods=['POST'])
def slideshow():
    files = request.form.getlist('filename')
    return render_template('slideshow.html', files=files)


@app.route("/", defaults={'path': '', 'remote': ''})
@app.route("/<string:remote>/", defaults={'path': ''})
@app.route("/<string:remote>/<path:path>")
def home(remote, path):
    if is_directory(remote, path):
        return show_directory(remote, path)

    return serve_file(remote, path)


if __name__ == "__main__":
    app.run(debug=True, threaded=True)
