import json
import pandas as pd
import sys


def parse_session(sess):
    """
    Parse a session into a summary.

    Args:
        sess: session as list of events
    Output:
        data: dict containing user_id, content_id, session_start, session_end,
            total_time, event_count, track_playtime and ad_count of the session
    """
    data = {}
    data['user_id'] = sess[0]['user_id']
    data['content_id'] = sess[0]['content_id']
    data['session_start'] = sess[0]['timestamp']
    data['session_end'] = sess[-1]['timestamp']
    data['total_time'] = data['session_end'] - data['session_start']
    data['event_count'] = len(sess)

    track_playtime = 0
    ad_count = 0
    paused = False
    started = False
    ended = False
    for event in sess:
        e_type = event['event_type']
        e_time = event['timestamp']

        if started and not paused and not ended:
            track_playtime += e_time - last_time
        if not started and e_type in ['track_start', 'track_hearbeat', 'play']:
            started = True
        if e_type == 'pause':
            paused == True
        if e_type == 'play':
            paused = False
        if e_type == 'track_end':
            ended = True
        last_time = e_time

        if e_type == 'ad_start':
            ad_count += 1

    data['track_playtime'] = track_playtime
    data['ad_count'] = ad_count
    print data


def stdin_iterator():
    """Iterate over the stdin stream."""
    while 1:
        try:
            inp = raw_input('')
        except EOFError:
            break
        yield inp


cur_sessions = {}
last_time = -sys.maxint
for event in stdin_iterator():
    e_data = json.loads(event)

    # Find sessions that have timeouted and remove them
    time = e_data['timestamp']
    if time > last_time:
        timeouted = filter(lambda sess_id: time - cur_sessions[sess_id][-1]['timestamp'] >= 60, cur_sessions)
        for sess_id in timeouted:
            parse_session(cur_sessions[sess_id])
            del cur_sessions[sess_id]
    last_time = time

    sess_id = e_data['user_id'] + e_data['content_id']
    if sess_id not in cur_sessions:
        cur_sessions[sess_id] = []

    # If getting stream_start event in middle of a stream start a new stream
    e_type = e_data['event_type']
    if e_type == 'stream_start' and len(cur_sessions[sess_id]):
        parse_session(cur_sessions[sess_id])
        cur_sessions[sess_id] = []

    cur_sessions[sess_id].append(e_data)

    if e_data['event_type'] == 'stream_end':
        parse_session(cur_sessions[sess_id])
        del cur_sessions[sess_id]
