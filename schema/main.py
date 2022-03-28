import os
import time

import pandas as pd
import datetime as dt
import yaml
import sys
sys.path.insert(0, '..')

from sklearn.manifold import TSNE

from sklearn.preprocessing import OrdinalEncoder as OE
from sklearn.preprocessing import OneHotEncoder as OH
from sklearn.preprocessing import LabelEncoder as LE

import numpy as np

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

from gremlin_python.process.traversal import Cardinality
from gremlin_python.process.traversal import T

import papermill as pm

# from utils.log import logger
from load.utils.log import logger
# from utils.data_loader import TableDataSet, VertexDataSet
# from gremlin import gremlin_instance

id = T.id
single = Cardinality.single


# # !pip install nest_asyncio
# import nest_asyncio
# nest_asyncio.apply()

def organize(_g, single):
    # organize cars
    print(f"organizing {_g.V().hasLabel('car_garage').count().next()} car nodes")
    for i, n in enumerate(_g.V().hasLabel('car_garage').toList()):
        node = _g.V(n).elementMap().next()
        old_keys = list(node.keys())[2:]
        node_type = 'car'

        vin = node.get("vin", 'N/A')
        odometer_value = node.get("mileage", 'N/A')
        odometer_unit = node.get("distance_unit", 'N/A')  # we're using distance_unit for odometer unit
        if odometer_unit == 'KM':
            odometer_unit = 'km'
        elif odometer_unit == 'M':
            odometer_unit = 'miles'
        odometer = {'value': odometer_value, 'unit': odometer_unit}
        year = node.get("model_year")
        make = node.get("make_name")
        model = node.get("model_name")
        trim = node.get("trim_name", 'N/A')
        location_country = node.get("vehicle_location_country", 'N/A')
        location_city = node.get("vehicle_location_city", 'N/A')
        location = {'country': location_country, 'city': location_city}
        price_value = node.get("price", 0)
        price_currency = node.get("currency", 'N/A')
        price = {'value': price_value, 'currency': price_currency}
        transmission = node.get("transmission", 'N/A').lower()
        l_or_r = node.get("l_or_r")
        if l_or_r == 'Left Hand Drive':
            steering = 'left'
        elif l_or_r == 'Right Hand Drive':
            steering = 'right'
        else:
            steering = 'N/A'
        externalId = node.get("id", 'N/A')
        inspection_value = 0
        inspection_provider = 'AIM' if 'aim_inspect' in node.keys() else 'N/A'
        inspection_created_at = dt.datetime.fromtimestamp(0)
        inspection = {'value': inspection_value, 'provider': inspection_provider, 'createdAt': inspection_created_at}
        manufactureYear = 0
        registrationYear = 0
        admissibleCountries = 'N/A'
        pictures = []
        status_id = int(node.get("status", 0))
        id_name_dict = {-1: "Deleted", 0: "N/A", 1: "Draft", 2: "Published", 3: "Archived", 4: "Sold", 5: "Locked",
                        6: "Template", 7: "Sale Pending"}
        status_name = id_name_dict[status_id]
        status = {'id': status_id, 'name': status_name}
        yearMakeModelTrim = f"{year}_{make}_{model}_{trim}"
        vinLastSix = vin[-6:]

        engineDisplacement_value = node.get('engine_type', 'N/A')  # not sure if engine_type can be a substitute for engineDisplacement_value
        engineDisplacement_units = 'N/A'
        engineDisplacement = {'value': engineDisplacement_value, 'units': engineDisplacement_units}
        fuel = node.get('fuel_type', 'N/A')
        distance_to_warehouse = str(node['distance_to_warehouse']) + ' ' + node['distance_unit']
        date = node['created_at']

        # remove all old values
        _g.V(n).properties(*old_keys).drop().toList()
        # insert new values
        _g.V(n).property(single, 'node_type', node_type).property('vin', vin).property('odometer', odometer).property(
            'year', year).property('make', make).property('model', model).property('trim', trim).property(
            'location', location).property('price', price).property('transmission', transmission).property(
            'steering', steering).property('externalId', externalId).property('inspection', inspection).property(
            'manufactureYear', manufactureYear).property('registrationYear', registrationYear).property(
            'admissibleCountries', admissibleCountries).property('pictures', pictures).property(
            'status', status).property('yearMakeModelTrim', yearMakeModelTrim).property(
            'vinLastSix', vinLastSix).property('engineDisplacement', engineDisplacement).property(
            'fuel', fuel).property('distance_to_warehouse', distance_to_warehouse).property('date', date).toList()

    # organize orders
    print(f"organizing {_g.V().hasLabel('order_order').count().next()} order nodes")
    for i, n in enumerate(_g.V().hasLabel('order_order').toList()):
        node = _g.V(n).elementMap().next()
        old_keys = list(node.keys())[2:]
        node_type = 'order'
        price = float(node.get("price", 0))

        # remove all old values
        _g.V(n).properties(*old_keys).drop().toList()
        # insert new values
        _g.V(n).property(single, 'node_type', node_type).property('price', price).toList()
    # organize edges
    print(f"organizing {_g.E().count().next()} edges")
    for i, e in enumerate(_g.E().toList()):
        edge = _g.E(e).elementMap().next()

        if 'edge_type' not in edge.keys():
            _g.E(e).property('edge_type', 'order').toList()


def get_TXMS(make, model, year, trim):
    TXMS_make = make.replace(' ', '_')
    TXMS_model = model.replace(' ', '_')
    TXMS_year = str(year)
    TXMS_trim = trim.replace(' ', '_')
    TXMS = f'{TXMS_make}.{TXMS_model}.{TXMS_year}.{TXMS_trim}'
    return TXMS


def add_TXMS_code(_g):
    for i, n in enumerate(g.V().hasLabel('car_garage').toList()):
        node = g.V(n).elementMap().next()
        make = node['make']
        model = node['model']
        year = node['year']
        trim = node['trim']
        TXMS = get_TXMS(make, model, year, trim)
        g.V(n).property(single, 'TXMS', TXMS).toList()


def add_car_type_nodes(_g, single):
    unique_TXMS = set([n['TXMS'] for n in g.V().hasLabel('car_garage').elementMap().toList()])
    for TXMS in unique_TXMS:
        _g.addV(TXMS).property(single, 'node_type', 'CarType').property('currency', 'N/A').property('price', '0').next()


def add_links_to_car_type_nodes(_g, single):
    for i, n in enumerate(_g.V().hasLabel('car_garage').toList()):
        node = _g.V(n).elementMap().next()
        try:
            TXMS = node['TXMS']
            n_cartype = _g.V().hasLabel(TXMS).next()
            _g.V(n_cartype).property(single, 'make', node['make']).property('model', node['model']).property('trim', node['trim']).property(
                'year', node['year']).toList()
            _g.V(n_cartype).addE(f'{TXMS}_{i}').to(_g.V(n).next()).property(single, 'edge_type', 'instance').toList()
        except:
            print('Error:\n', node)


def graph_to_df_cartype(_g):
    _df = pd.DataFrame(columns=['TXMS', 'make', 'model', 'year', 'trim'])
    for i, n in enumerate(_g.V().has('node_type', 'CarType').elementMap().toList()):
        txms = n[list(n.keys())[1]]
        _df.loc[i] = [txms, n['make'], n['model'], n['year'], n['trim']]
    return _df.reset_index(drop=True)


def get_tsne(_df):
    _df['labels'] = _df.year.astype(str) + '_' + _df.make + '_' + _df.model + '_' + _df.trim

    ord_enc = OE()
    one_hot = OH(sparse=False)
    le = LE()
    make = one_hot.fit_transform(_df[['make']])
    model = one_hot.fit_transform(_df[['model']])
    trim = one_hot.fit_transform(_df[['trim']])
    year = ord_enc.fit_transform(_df[['year']])
    feature_mat = np.concatenate((make, model, year, trim), axis=1)
    X_tsne = TSNE(learning_rate=100).fit_transform(feature_mat)
    return X_tsne


def k_closest(node, nodes, k):
    nodes = np.asarray(nodes)
    dist_2 = np.sum((nodes - node)**2, axis=1)
    return np.argsort(dist_2)[1:k+1]


def add_similarity_links(_g, _df):
    for i in range(_df.shape[0]):
        closest = k_closest(X_tsne[i], X_tsne, 5)
        txms1 = _df['TXMS'].loc[i]
        node1 = _g.V().hasLabel(txms1).next()
        for i, close in enumerate(closest):
            txms2 = _df['TXMS'].loc[close]
            _g.V(node1).addE(f'{txms1}_{txms2}_{i}').to(_g.V().hasLabel(txms2).next()).property(
                single, 'edge_type', 'similar').property('similar_rank', i).toList()


# Gather the price for each car type and compute the average price - working directly on graph
def get_cartype_eval_traverse(_g, after=None, before=None):
    _eval_df_from_traverse = pd.DataFrame(
        columns=['txms', 'make', 'model', 'year', 'trim', 'price', 'price_ca', 'price_us', 'currency'])
    for i, n in enumerate(_g.V().has('node_type', 'CarType').toList()):
        node = _g.V(n).elementMap().next()
        # overall price
        cars_nodes = _g.V(n).outE().has('edge_type', 'instance').inV().has('node_type', 'car').elementMap().toList()
        prices = []
        for car_node in cars_nodes:
            if after and dt.datetime.fromisoformat(car_node['date']) < dt.datetime.fromisoformat(after):
                continue
            if before and dt.datetime.fromisoformat(car_node['date']) > dt.datetime.fromisoformat(before):
                continue
            #             if car_node['currency'] != 'USD':
            #                 print('covert to USD')
            prices.append(float(car_node['price']['value']))
        prices = np.array(prices)
        prices_without_zeros = prices[prices != 0]

        if list(prices_without_zeros) == []:
            avg_price = 0
        else:
            avg_price = np.mean(prices_without_zeros)

        # ca price
        cars_nodes = _g.V(n).outE().has('edge_type', 'instance').inV().has('node_type', 'car').elementMap().toList()
        cars_nodes = [c for c in cars_nodes if c['location']['country'] == 'CA']
        prices = []
        for car_node in cars_nodes:
            if after and dt.datetime.fromisoformat(car_node['date']) < dt.datetime.fromisoformat(after):
                continue
            if before and dt.datetime.fromisoformat(car_node['date']) > dt.datetime.fromisoformat(before):
                continue
            #             if car_node['currency'] != 'USD':
            #                 print('covert to USD')
            prices.append(float(car_node['price']['value']))
        prices = np.array(prices)
        prices_without_zeros = prices[prices != 0]

        if list(prices_without_zeros) == []:
            avg_price_ca = 0
        else:
            avg_price_ca = np.mean(prices_without_zeros)
        # us price
        cars_nodes = _g.V(n).outE().has('edge_type', 'instance').inV().has('node_type', 'car').elementMap().toList()
        cars_nodes = [c for c in cars_nodes if c['location']['country'] == 'US']
        prices = []
        for car_node in cars_nodes:
            if after and dt.datetime.fromisoformat(car_node['date']) < dt.datetime.fromisoformat(after):
                continue
            if before and dt.datetime.fromisoformat(car_node['date']) > dt.datetime.fromisoformat(before):
                continue
            #             if car_node['currency'] != 'USD':
            #                 print('covert to USD')
            prices.append(float(car_node['price']['value']))
        prices = np.array(prices)
        prices_without_zeros = prices[prices != 0]

        if list(prices_without_zeros) == []:
            avg_price_us = 0
        else:
            avg_price_us = np.mean(prices_without_zeros)

        txms = node[list(node.keys())[1]]
        _eval_df_from_traverse.loc[i] = [txms, node['make'], node['model'], node['year'], node['trim'], avg_price,
                                         avg_price_ca, avg_price_us, 'tmp USD']
    _eval_df_from_traverse = _eval_df_from_traverse.reset_index(drop=True)
    return _eval_df_from_traverse


def get_historical_evaluations_df(_g, from_date, duration_steps, step_days):
    eval_df_sample_traverse_total = pd.DataFrame()
    base = dt.datetime.fromisoformat(from_date)
    dates = [(base + dt.timedelta(days=x * step_days)).isoformat() for x in range(duration_steps)]
    for i, date in enumerate(dates):
        print(f'Step {i + 1}/{len(dates)} Evaluating before: {date}')
        eval_df_from_traverse = get_cartype_eval_traverse(_g, before=date)
        eval_df_from_traverse['value_at'] = date
        eval_df_sample_traverse_total = eval_df_sample_traverse_total.append(eval_df_from_traverse, ignore_index=True)
    return eval_df_sample_traverse_total


def cartype_add_multiframe_timeseries(_g, time, txms, value, value_ca, value_us, single):
    year = time[0:4]
    month = time.replace('-', '')[0:6]
    day = time.replace('-', '')[0:8]

    # if year node doesn't exist
    if not len(_g.V().hasLabel(year).toList()):
        _g.addV(year).property(id, year).property(single, 'node_type', 'datetime').property(
            'datetime_type', 'year').property('datetime', time).next()
    # if month node doesn't exist
    if not len(_g.V().hasLabel(month).toList()):
        _g.addV(month).property(id, month).property(single, 'node_type', 'datetime').property(
            'datetime_type', 'month').property('datetime', month).next()
        _g.V(month).addE(month).to(_g.V(year).next()).property(id, month).property('edge_type', 'month_year').toList()
    # if day node doesn't exist
    if not len(_g.V().hasLabel(day).toList()):
        _g.addV(day).property(id, day).property(single, 'node_type', 'datetime').property(
            'datetime_type', 'day').property('datetime', day).next()
        _g.V(day).addE(day).to(_g.V(month).next()).property(id, day).property('edge_type', 'day_month').toList()

    _g.V(_g.V().hasLabel(txms).next()).addE(txms + day).to(_g.V(day).next()).property(single, 'value', value).property(
        'value_ca', value_ca).property('value_us', value_us).property('edge_type', 'CarType_time').property(
        'time_type', 'day').property('month', month).toList()

    avg_days = np.mean([l['value'] for l in
                        g.V().hasLabel(txms).outE().has('time_type', 'day').has('month', month).elementMap().toList()])
    avg_days_ca = np.mean([l['value_ca'] for l in g.V().hasLabel(txms).outE().has('time_type', 'day').has('month',
                                                                                                          month).elementMap().toList()])
    avg_days_us = np.mean([l['value_us'] for l in g.V().hasLabel(txms).outE().has('time_type', 'day').has('month',
                                                                                                          month).elementMap().toList()])

    if not len(_g.E().hasLabel(txms + month).toList()):
        _g.V(_g.V().hasLabel(txms).next()).addE(txms + month).to(_g.V(month).next()).property(single, 'value',
                                                                                              avg_days).property(
            'value_ca', avg_days_ca).property('value_us', avg_days_us).property('edge_type', 'CarType_time').property(
            'time_type', 'month').property('year', year).toList()
    else:
        _g.E(_g.E().hasLabel(txms + month).toList()).property('value', avg_days).property(
            'value_ca', avg_days_ca).property('value_us', avg_days_us).toList()

    avg_months = np.mean([l['value'] for l in g.V().hasLabel(txms).outE().has('time_type', 'month').has(
        'year', year).elementMap().toList()])
    avg_months_ca = np.mean([l['value_ca'] for l in g.V().hasLabel(txms).outE().has('time_type', 'month').has(
        'year', year).elementMap().toList()])
    avg_months_us = np.mean([l['value_us'] for l in g.V().hasLabel(txms).outE().has('time_type', 'month').has(
        'year', year).elementMap().toList()])

    if not len(_g.E().hasLabel(txms + year).toList()):
        _g.V(_g.V().hasLabel(txms).next()).addE(txms + year).to(_g.V(year).next()).property(
            single, 'value', avg_months).property('value_ca', avg_months_ca).property(
            'value_us', avg_months_us).property('edge_type', 'CarType_time').property('time_type', 'year').toList()
    else:
        _g.E(_g.E().hasLabel(txms + year).toList()).property('value', avg_months).property(
            'value_ca', avg_months_ca).property('value_us', avg_months_us).toList()


def add_historical_evaluations(_df, single, verbose=False):
    for i, r in _df.iterrows():
        if verbose and i % 1000 == 0:
            print(f'{i}/{_df.shape[0]}')
        time = r['value_at']
        txms = r['txms']
        value = r['price']
        value_ca = r['price_ca']
        value_us = r['price_us']
        cartype_add_multiframe_timeseries(g, time, txms, value, value_ca, value_us, single)


with open("config.yaml", "r") as stream:
    try:
        conf = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

# Connect to Gremlin Server
try:
    conn_gremlin = DriverRemoteConnection('ws://{}:{}/gremlin'.format(conf["GREMLIN_HOST"], conf["GREMLIN_PORT"], 'g'))
    g = traversal().withRemote(conn_gremlin)
except Exception as e:
    print("Connect to Gremlin Server failed, {}".format(e))
    exit()


try:
    # wait till vertices are loaded
    logger.info("waiting till vertices are loaded")
    still_loading = True
    time.sleep(5)
    while still_loading:
        vertices_count1 = g.V().count().next()
        time.sleep(5)
        vertices_count2 = g.V().count().next()
        if vertices_count1 == vertices_count2:
            still_loading = False

    # wait till edges are loaded
    logger.info("waiting till edges are loaded")
    still_loading = True
    while still_loading:
        edges_count1 = g.E().count().next()
        time.sleep(5)
        edges_count2 = g.E().count().next()
        if edges_count1 == edges_count2:
            still_loading = False

    # organize
    logger.info("Organizing Vertices Data")
    organize(g, single)

    # Add TXMS
    logger.info("Adding TXMS code")
    add_TXMS_code(g)

    # Add CarType
    logger.info("Adding CarType Vertices")
    add_car_type_nodes(g, single)

    # Add CarType Edges
    logger.info("Adding CarType Edges")
    add_links_to_car_type_nodes(g, single)

    # Convert Graph to Dataframe
    logger.info("Converting Graph to Dataframe")
    df_graph = graph_to_df_cartype(g)

    # Get Similarity Among CarTypes
    logger.info("Getting the TSNE (Similarity) Among CarTypes")
    X_tsne = get_tsne(df_graph)

    # Add Similarity Edges
    logger.info("Add Similarity Edges Among CarTypes")
    add_similarity_links(g, df_graph)

    # Get Historical Evaluations of CarTypes (TXMSs) in Dataframe
    logger.info("Getting Historical Evaluations of CarTypes (TXMSs) in Dataframe")

    eval_df = get_historical_evaluations_df(g, from_date=conf['HISTORICAL_EVALUATIONS_FROM'],
                                            duration_steps=conf['HISTORICAL_EVALUATIONS_DURATION_STEPS'],
                                            step_days=conf['HISTORICAL_EVALUATIONS_STEP_DAYS'])

    # Add Historical Evaluations of CarTypes (TXMSs) Vertices and Edges
    logger.info("Adding Historical Evaluations of CarTypes (TXMSs) Vertices and Edges")
    add_historical_evaluations(eval_df, single, verbose=True)

    logger.info("Succeeded")

    # Run JupyterDash, then Sleep Indefinitely for JupyterDash to stay running
    logger.info("Running JupyterDash on http://127.0.0.1:8050/, then Sleeping Indefinitely for JupyterDash to stay running")
    pm.execute_notebook(
        'finansial_asset_vs_vehicle_asset_in_jupyterdash.ipynb',
        'finansial_asset_vs_vehicle_asset_in_jupyterdash_executed.ipynb',
        parameters=dict(in_papermill=True, gremlin_host=conf["GREMLIN_HOST"], gremlin_port=conf["GREMLIN_PORT"])
    )
except Exception as e:
    logger.error(e)
    sys.exit()
