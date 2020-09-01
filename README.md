# Postgresql

## Example 1
connection_pool = database.Connection(
        database = os.environ["database"],
        user = os.environ["dbusername"],
        password = os.environ["dbpwd"],
        host = os.environ["dbhost"],
        port = 5432)

with database.Database(connection_pool) as db:
    data = db.fetchall("ugcdb.vwProductSubRatingsStatistics",fields=['ratingtype','avgrating','labeltext','attributename'], \
            where=("countrycode=%s and productid=%s and locale=%s",[stat['countrycode'],stat['productid'],stat['locale']]))
        for sr in data:
            secondaryRatings[sr['attributename']] = {
                    'id' : sr['attributename'],
                    .
                    .
                    .
                }

## Example 2
connection_pool = database.Connection(
        database = os.environ["database"],
        user = os.environ["dbusername"],
        password = os.environ["dbpwd"],
        host = os.environ["dbhost"],
        port = 5432)

with database.Database(connection_pool) as db:
    db.call("schema.your_proc_name")
    db.commit()

## Example 3
with database.Database(connection_pool) as db:
        reviewer_data = {
                'reviewerid': str(uuid.uuid4()),
                'externalReviewerId': review['reviewer']['reviewerExternalId'],
                'reviewerDisplayName': review['reviewer']['reviewerDisplayName'],
                'reviewerNickname': review['reviewer']['reviewerNickname'],
                'anonymousReviewer': False,
                'verifiedPurchaser': review['reviewer']['verifiedPurchaser'],
                'reviewerEmailAddress': review['reviewer']['reviewerEmailAddress'],
                'socialImageUrl': review['reviewer']['socialImageUrl'],
                'insertDatetime': insertDatetime,
                'lastModificationDateTime': insertDatetime
        }
        # reviewer data load
        cursor = db.mergeupdate('ugcdb.productReviewers',reviewer_data,["reviewerEmailAddress","externalReviewerId"],["lastModificationDateTime"],"reviewerid")
        id_of_new_row = json.loads(json.dumps(cursor))

## Example 5
with database.Database(connection_pool) as db:
        data = {
                'productId' : p['productId'],
                'productType' : p['productType'],
                .,
                .,
                .,
                }
        db.insert('schema.table_name',data=data)
        db.commit()