import os

from flask import Flask

from flask.ext import admin
from flask.ext.mongoengine import MongoEngine
from flask.ext.admin.contrib.mongoengine import ModelView

# Create application
app = Flask(__name__)
app.config['DEBUG'] = True
# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'
app.config['MONGODB_SETTINGS'] = {
    'db': os.environ.get('MONGODB_DATABASE', 'ai'),
    'username': os.environ.get('MONGODB_USERNAME', None),
    'password': os.environ.get('MONGODB_PASSWORD', None),
    'host': os.environ.get('MONGODB_HOST', None),
    'port': int(os.environ.get('MONGODB_PORT', 27017)),
}

# Create models
db = MongoEngine()
db.init_app(app)


# Define mongoengine documents
class Attribute(db.EmbeddedDocument):
    name = db.StringField()
    value = db.StringField()


class Report(db.Document):
    date = db.StringField()
    category = db.StringField()
    activity_id = db.IntField()
    activity = db.StringField()
    partner_id = db.IntField()
    partner_name = db.StringField()
    location_id = db.IntField()
    location_name = db.StringField()
    location_x = db.DecimalField()
    location_y = db.DecimalField()
    indicator_id = db.IntField()
    indicator_name = db.StringField()
    value = db.DecimalField()
    comments = db.StringField()
    attributes = db.ListField(
        db.EmbeddedDocumentField(Attribute)
    )


# Customized admin views
class ReportView(ModelView):
    can_create = False
    can_delete = False
    can_edit = False

    column_filters = [
        'date',
        'category',
        'activity',
        'partner_name',
        'location_name',
        'indicator_name',
    ]

    column_searchable_list = column_filters

    form_subdocuments = {
        'attributes': {
            'form_subdocuments': {
                None: {
                    'form_columns': ('name', 'value',)
                }
            }

        }
    }


# Create admin
admin = admin.Admin(app, 'ActivityInfo Reports')

# Add views
admin.add_view(ReportView(Report))


# Flask views
@app.route('/')
def index():
    return '<a href="/admin/">Click me to get to Admin!</a>'


if __name__ == '__main__':

    # Start app
    app.run(debug=True)