#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'jcranwellward'

import os, re
import random
import datetime

from flask.ext.script import (
    Command,
    Manager,
    Option,
    Server
)

from pymongo import MongoClient
from activtyinfo_client import ActivityInfoClient
from cartodb import CartoDBAPIKey, CartoDBException

from aggregator import app, Report, Attribute, db

manager = Manager(app)

ai = MongoClient(
    os.environ.get('MONGO_URL', 'mongodb://localhost:27017'))[
    os.environ.get('MONGODB_DATABASE', 'ai')]




@manager.command
def update_sites(
        api_key='cad5c2fd1aa5236083743f54264b203d903f3a06',
        domain='unhcr',
        table_name='imap_v5_cadcode',
        site_type='IS',
        name_col='pcodename',
        code_col='p_code',
    ):

    client = CartoDBAPIKey(api_key, domain)

    sites = client.sql(
        'select * from {}'.format(table_name)
    )

    for row in sites['rows']:
        p_code = row[code_col]
        site_name = row[name_col].encode('UTF-8')
        cad = ai['Cadastral Area'].find_one({'code': row['cad_code']})
        caz = ai['Caza'].find_one({'id': cad['parentId']})
        gov = ai['Governorate'].find_one({'id': caz['parentId']})

        location = ai.locations.find_one({'p_code': p_code})
        if not location:
            location = {
                "p_code": p_code,
                "ai_id": int(random.getrandbits(31))  # (31-bit random key),
            }

        location["ai_name"] = '{}: {}'.format(site_type, site_name)
        location["name"] = site_name
        location["type"] = site_type
        location["latitude"] = row['latitude']
        location["longitude"] = row['longitude']
        location["adminEntities"] = {
            str(gov['levelId']): {
                "id": gov['id'],
                "name": gov['name']
            },
            str(caz['levelId']): {
                "id": caz['id'],
                "name": caz['name']
            },
            str(cad['levelId']): {
                "id": cad['id'],
                "name": cad['name']
            },
        }

        ai.locations.update({'p_code': p_code}, location, upsert=True)
        print 'Updated {}: {}'.format(site_type, site_name)


@manager.command
def update_ai_locations(type_id, username='', password=''):

    client = ActivityInfoClient(username, password)

    for location in ai.locations.find():

        payload = {
            'id': location['ai_id'],
            'locationTypeId': type_id,
            'name': location['ai_name'],
            'axe': '{}: {}'.format('PCode', location['p_code']),
            'latitude': location['latitude'],
            'longitude': location['longitude'],
            'workflowstatusid': 'validated'
        }
        for id, level in location['adminEntities'].items():
            payload['E{}'.format(id)] = level['id']

        response = client.call_command('CreateLocation', **payload)
        print response


@manager.command
def import_ai(ai_db, username='', password=''):
    """
    Imports data from Activity Info
    """

    db_id = ai_db
    client = ActivityInfoClient(username, password)

    ai = MongoClient(
        os.environ.get('MONGO_URL', 'mongodb://localhost:27017'))[
        os.environ.get('MONGODB_DATABASE', 'ai')]

    # store the whole database for future reference
    print u'Pulling database...'
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
        print u'Pulling sites for activity: {} - {}'.format(activity['id'], activity['name'])
        sites = client.get_sites(activity=activity['id'], include_monthly_reports=False)
        for site in sites:
            attributes = [attr for attr in ai.attributeGroups.find(
                {'attributes.id': {'$in': site['attributes']}},
                {'name': 1, 'mandatory': 1, "attributes.$": 1}
            )]

            print '     Pulling reports for site: {} - {}'.format(
                site['id'],
                site['location']['name'].encode('UTF-8')
            )
            reports = client.get_monthly_reports_for_site(site['id'])
            for date, indicators in reports.items():
                for indicator in indicators:
                    report, created = Report.objects.get_or_create(
                        date=date,
                        site_id=site['id'],
                        activity_id=activity['id'],
                        partner_id=site['partner']['id'],
                        indicator_id=indicator['indicatorId'],
                    )
                    report.value = indicator['value']
                    report.category = activity['category']
                    report.activity = activity['name']
                    report.partner_name = site['partner']['name']
                    report.location_name = site['location']['name']
                    report.location_id = site['location']['id']
                    report.location_x = site['location'].get('longitude', None)
                    report.location_y = site['location'].get('latitude', None)
                    report.indicator_name = indicator['indicatorName']
                    report.comments = site.get('comments', None)

                    location = ai.locations.find_one({'ai_id': report.location_id})
                    if location:
                        report.p_code = location['p_code']
                    elif report.comments:
                        matches = re.search(r'(\d{5}-\d?\d-\d{3})', report.comments)
                        if matches:
                            report.p_code = matches.group(1)

                    if created:
                        for a in attributes:
                            report.attributes.append(
                                Attribute(
                                    name=a['name'],
                                    value=a['attributes'][0]['name']
                                )
                            )

                        print '        Created report: {} -> {} -> {} -> {} = {}'.format(
                            report.date,
                            report.location_name.encode('UTF-8'),
                            report.partner_name.encode('UTF-8'),
                            report.indicator_name.encode('UTF-8'),
                            report.value
                        )

                    report.save()


# Turn on debugger by default and reloader
manager.add_command("runserver", Server(
    use_debugger=True,
    use_reloader=True,
    host='0.0.0.0')
)


if __name__ == "__main__":
    manager.run()
