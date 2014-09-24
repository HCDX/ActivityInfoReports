import os
import StringIO
import datetime

from pandas import DataFrame

from flask import Flask
from flask import redirect
from flask import request
from flask import send_file
from flask.ext import admin
from flask.ext.mongoengine import MongoEngine
from flask.ext.admin.contrib.mongoengine import ModelView
from flask.ext.admin import expose
from flask.ext.mongorest import MongoRest
from flask.ext.mongorest.views import ResourceView
from flask.ext.mongorest.resources import Resource
from flask.ext.mongorest import operators as ops
from flask.ext.mongorest import methods


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
    site_id = db.IntField()
    p_code = db.StringField()
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
    list_template = 'list.html'

    column_filters = [
        'date',
        'p_code',
        'category',
        'activity',
        'partner_name',
        'location_name',
        'indicator_name',
        'comments',
    ]
    column_list = [
        'date',
        'category',
        'activity',
        'partner_name',
        'location_name',
        'p_code',
        'indicator_name',
        'value',
        'comments',
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

    @expose('/export')
    def export(self):
        # Grab parameters from URL
        page, sort_idx, sort_desc, search, filters = self._get_list_extra_args()
        sort_column = self._get_column_by_idx(sort_idx)
        if sort_column is not None:
            sort_column = sort_column[0]

        # Get count and data
        self.page_size = request.args.get('count', 0, type=int)
        count, data = self.get_list(
            None,
            sort_column,
            sort_desc,
            search,
            filters
        )

        dicts = []
        for report in data:
            dicts.append(report.to_mongo().to_dict())
        df = DataFrame.from_records(dicts, columns=self.column_list)

        buffer = StringIO.StringIO()  # use stringio for temp file
        df.to_csv(buffer, encoding='utf-8')
        buffer.seek(0)

        filename = "ai_reports_" + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M") + ".csv"
        return send_file(
            buffer,
            attachment_filename=filename,
            as_attachment=True,
            mimetype='text/csv'
        )


# Create admin
admin = admin.Admin(app, 'ActivityInfo Reports')

# Add views
admin.add_view(ReportView(Report))

# Add API
api = MongoRest(app)


class AttributeResource(Resource):
    document = Attribute


class ReportResource(Resource):
    document = Report
    related_resources = {
        'attributes': AttributeResource,
    }
    filters = {
        'partner_name': [ops.Exact, ops.Startswith],
    }

@api.register(name='reports', url='/reports/')
class ReportsView(ResourceView):
    resource = ReportResource
    methods = [methods.List]

# Flask views
@app.route('/')
def index():
    return redirect('/admin')


if __name__ == '__main__':

    # Start app
    app.run(debug=True)