#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'jcranwellward'

import logging
import os, re
import random
import datetime
import requests
import json

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


def send_message(message):
    requests.post(
        'https://hooks.slack.com/services/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',
        data=json.dumps({'text': message})
    )


@manager.command
def update_levels(country_code='LB'):
    """
    Updates local admin level lookup tables from AI.
    These lookup tables are used when creating sites for AI.
    """

    client = ActivityInfoClient()

    for level in client.get_admin_levels(country_code):
        entities = client.get_entities(level['id'])
        for entity in entities:
            ai[level['name']].update(
                {'_id': entity['id']}, entity, upsert=True)
            print 'Updated entity {}: {}'.format(
                level['name'], entity['name'].encode('UTF-8')
            )

    for site_type in client.get_location_types(country_code):
        locations = client.get_locations(site_type['id'])
        for location in locations:
            ai.locations.update(
                {'_id': location['id']}, location, upsert=True)
            print 'Updated {}: {}'.format(
                site_type['name'], location['name'].encode('UTF-8')
            )


@manager.command
def update_sites(
    api_key='cad5c2fd1aa5236083743f54264b203d903f3a06',
    domain='unhcr',
    username='jcranwellward@unicef.org',
    password='Inn0vation',
    list_name='ai_localities',
    site_type='LOC',
    name_col='location_name_en',
    code_col='pcode',
    target_list='51048'
):

    carto_client = CartoDBAPIKey(api_key, domain)

    ai_client = ActivityInfoClient(username, password)

    # create an index of sites by p_code
    existing = dict(
        (site['code'], dict(site, index=i))
        for (i, site) in enumerate(
            ai_client.get_locations(target_list)
        )
    )

    sites = carto_client.sql(
        'select * from {}'.format(list_name)
    )

    bad_codes = []
    updated_sites = 0
    for row in sites['rows']:
        p_code = row[code_col]
        site_name = row[name_col].encode('UTF-8')
        cad = ai['Cadastral Area'].find_one({'code': str(row['cad_code'])})
        if cad is None:
            bad_codes.append(row['cad_code'])
            continue
        caz = ai['Caza'].find_one({'id': cad['parentId']})
        gov = ai['Governorate'].find_one({'id': caz['parentId']})

        if p_code not in existing:

            payload = dict(
                id=int(random.getrandbits(31)),
                locationTypeId=target_list,
                name='{}: {}'.format(site_type, site_name),
                axe='{}'.format(p_code),
                latitude=row['latitude'],
                longitude=row['longitude'],
                workflowstatusid='validated'
            )
            payload['E{}'.format(gov['levelId'])] = gov['id']
            payload['E{}'.format(caz['levelId'])] = caz['id']
            payload['E{}'.format(cad['levelId'])] = cad['id']

            response = ai_client.call_command('CreateLocation', **payload)
            if response.status_code == requests.codes.ok:
                updated_sites += 1
                print 'Updated {}: {}'.format(site_type, site_name)
            else:
                print 'Error for {}: {}'.format(site_type, site_name)

    print 'Bad codes: {}'.format(bad_codes)
    print 'Updated sites: {}'.format(updated_sites)


@manager.command
def update_ai_locations(type_id, username='', password=''):

    client = ActivityInfoClient(username, password)

    updated_location = 0
    for location in ai.locations.find({'ai_name': {'$regex': 'PG'}}):

        payload = {
            'id': int(random.getrandbits(31)),
            'locationTypeId': type_id,
            'name': location['ai_name'],
            'axe': '{}'.format(location['p_code']),
            'latitude': location['latitude'],
            'longitude': location['longitude'],
            'workflowstatusid': 'validated'
        }
        for id, level in location['adminEntities'].items():
            payload['E{}'.format(id)] = level['id']

        response = client.call_command('CreateLocation', **payload)
        if response.status_code == requests.codes.ok:
            updated_location += 1
            print 'Uploaded {}'.format(location['ai_name'].encode('UTF-8'))
        else:
            print 'Error for: {}'.format(location['ai_name'].encode('UTF-8'))

    print updated_location

@manager.command
def import_ai(username='', password=''):
    """
    Imports data from Activity Info
    """

    reports_created = 0
    db_ids = os.environ.get('AI_DB_IDS').split()
    client = ActivityInfoClient(username, password)

    for db_id in db_ids:
        # store the whole database for future reference
        print u'Pulling database...'
        db_info = client.get_database(db_id)
        send_message('AI import started for database: {}'.format(db_info['name']))

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
                attributes = []
                if 'attributes' in site:
                    attributes = [attr for attr in ai.attributeGroups.find(
                        {'attributes.id': {'$in': site['attributes']}},
                        {'name': 1, 'mandatory': 1, "attributes.$": 1}
                    )]

                print '     Pulling reports for site: {} - {}'.format(
                    site['id'],
                    site['location']['name'].encode('UTF-8')
                )
                try:
                    reports = client.get_monthly_reports_for_site(site['id'])
                    for date, indicators in reports.items():
                        for indicator in indicators:
                            report, created = Report.objects.get_or_create(
                                db_name=db_info['name'],
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

                            location = ai.locations.find_one({'_id': report.location_id})
                            if location:
                                if 'code' in location:
                                    report.p_code = location['code']
                                try:
                                    report.gov_code = str(location['adminEntities']['1370']['id'])
                                    report.governorate = location['adminEntities']['1370']['name']
                                    report.district_code = str(location['adminEntities']['1521']['id'])
                                    report.district = location['adminEntities']['1521']['name']
                                    report.cadastral_code = str(location['adminEntities']['1522']['id'])
                                    report.cadastral = location['adminEntities']['1522']['name']
                                except Exception as exp:
                                    send_message('AI import error, location {}'.format(exp))

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

                                report.save()
                                reports_created += 1

                except Exception as exp:
                    send_message('AI import error, {}'.format(exp))
                    continue

        send_message('AI import finished, {} site reports created'.format(reports_created))


# Turn on debugger by default and reloader
manager.add_command("runserver", Server(
    use_debugger=True,
    use_reloader=True,
    host='0.0.0.0')
)


if __name__ == "__main__":
    manager.run()
