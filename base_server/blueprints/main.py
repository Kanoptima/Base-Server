""" Provides general blueprints and main index page """

from celery.result import AsyncResult
from flask import Blueprint, jsonify, render_template
from flask_login import current_user  # type: ignore

from base_server.helpers.auth import staff_login_required
from base_server.models.user import User

bp = Blueprint('main', __name__)

current_user: User


@bp.route('/')
@staff_login_required
def index():
    """ Renders home page. Uses current_user.home_pages() method to find options for the button grid. """
    return render_template('index.html',
                           user_name=current_user.name,
                           web_pages=current_user.most_used_pages(10))


@bp.route('/task-status/<task_id>', methods=['GET'])
@staff_login_required
def task_status(task_id):
    """ Checks status of task with id `task_id`, returns result to browser to inform on whether to navigate to output page. """
    task = AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'progress': 0,
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'progress': 100 if task.state == 'SUCCESS' else 50,
            # 'result': task.info,  # Task result
        }
    else:
        # Something went wrong
        response = {
            'state': task.state,
            'progress': 100,
            'result': str(task.info),  # Exception information
        }
    return jsonify(response)


@bp.route('/uncaught_error')
def uncaught_error():
    """ Shows 500 error screen. """
    return render_template('error/500.html')


@bp.route('/bad_email')
def bad_email():
    """ Shows bad email error screen. """
    return render_template('error/bad_email.html')
