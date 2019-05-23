from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/<int:model>/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(model, user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_rated = recommendation_engine.get_top_ratings(model, user_id, count)
    return json.dumps(top_rated)


@main.route("/<int:model>/products/<int:product_id>/recommend/<int:count>", methods=["GET"])
def product_recommending(model, product_id, count):
    logger.debug("ProductId %s TOP user recommending", product_id)
    top_rated = recommendation_engine.get_top_product_recommend(model, product_id, count)
    return json.dumps(top_rated)


@main.route("/<int:model>/<int:user_id>/ratings/<int:product_id>", methods=["GET"])
def product_ratings(model, user_id, product_id):
    logger.debug("User %s rating requested for product %s", user_id, product_id)
    ratings = recommendation_engine.get_ratings_for_product_ids(model, user_id, product_id)
    return json.dumps(ratings)

def create_app(spark_session, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
