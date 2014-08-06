__author__ = 'jcranwellward'

import datetime

from flask.ext.script import (
    Command,
    Manager,
    Option,
    Server
)

from pymongo import MongoClient
from activtyinfo_client import ActivityInfoClient

from aggregator import app, Report, Attribute, db

manager = Manager(app)

@manager.command
def import_ai(ai_db, username='', password=''):
    """
    Imports data from Activity Info
    """

    try:
        db_id = ai_db
        client = ActivityInfoClient(username, password)

        print app.config['MONGODB_SETTINGS']
        print db.connection.database_names()
        ai = db.connection[app.config['MONGODB_SETTINGS']['db']]
        # store the whole database for future reference
        print 'Pulling database...'
        db_info = client.get_database(db_id)
        ai.databases.update({'_id': db_id}, db_info, upsert=True)
        # split out all the attribute groups into a separate collection
        attribs = ai.databases.aggregate([
            {'$project': {'groups': '$activities.attributeGroups'}},
            {'$unwind': '$groups'},
            {'$unwind': '$groups'},
            {'$group': {'_id': "$_id", 'groups': {'$push': '$groups'}}},
        ])
        for attrib in attribs['result'][0]['groups']:
            ai.attributeGroups.update({'_id': attrib['id']}, attrib, upsert=True)

        for activity in ai.databases.find_one({'_id': db_id})['activities']:
            print 'Pulling sites for activity: {} - {}'.format(activity['id'], activity['name'])
            sites = client.get_sites(activity=activity['id'], include_monthly_reports=False)
            for site in sites:
                attributes = [attr for attr in ai.attributeGroups.find(
                    {'attributes.id': {'$in': site['attributes']}},
                    {'name': 1, 'mandatory': 1, "attributes.$": 1}
                )]

                print 'Pulling reports for site: {} - {}'.format(
                    site['id'],
                    site['location']['name'].encode('UTF-8')
                )
                reports = client.get_monthly_reports_for_site(site['id'])
                for date, indicators in reports.items():
                    for indicator in indicators:
                        report, created = Report.objects.get_or_create(
                            date=date,
                            category=activity['category'],
                            activity_id=activity['id'],
                            activity=activity['name'],
                            partner_id=site['partner']['id'],
                            partner_name=site['partner']['name'],
                            location_id=site['location']['id'],
                            location_name=site['location']['name'],
                            location_x=site['location'].get('longitude', None),
                            location_y=site['location'].get('latitude', None),
                            indicator_id=indicator['indicatorId'],
                            indicator_name=indicator['indicatorName'],
                            value=indicator['value'],
                            comments=site.get('comments', None)
                        )
                        if created:
                            for a in attributes:
                                report.attributes.append(
                                    Attribute(
                                        name=a['name'],
                                        value=a['attributes'][0]['name']
                                    )
                                )
                            report.save()
                            print 'Created report: {} -> {} -> {} -> {} = {}'.format(
                                report.date,
                                report.location_name,
                                report.partner_name,
                                report.indicator_name,
                                report.value
                            )
    except Exception as exp:
        print exp.message.encode('utf-8')


# Turn on debugger by default and reloader
manager.add_command("runserver", Server(
    use_debugger=True,
    use_reloader=True,
    host='0.0.0.0')
)


if __name__ == "__main__":
    manager.run()
