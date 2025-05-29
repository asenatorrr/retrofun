from datetime import datetime

from sqlalchemy import func, or_, select

from db import Session
from models import Manufacturer, Product, Country, Order, OrderItem, Customer, ProductReview

with Session() as session:
    with session.begin():
        # 1
        sale_amount = func.sum(OrderItem.quantity * OrderItem.unit_price).label(None)
        q = (select(Order, sale_amount)
             .join(Order.order_items)
             .group_by(Order)
             .having(sale_amount > 300)
             .order_by(sale_amount.desc()))
        print(len(session.execute(q).all()))

        # 2
        q = (select(Order)
             .join(Order.order_items)
             .join(OrderItem.product)
             .where(Product.name.like('%ZX81%'))
             .distinct())
        print(session.scalars(q).all())

        # 3
        q = (select(Order)
             .join(Order.order_items)
             .join(OrderItem.product)
             .join(Product.manufacturer)
             .where(Manufacturer.name == 'Amstrad')
             .distinct())
        print(len(session.scalars(q).all()))

        # 4
        q = (select(Order)
             .join(Order.order_items)
             .where(Order.timestamp.between(datetime(2022, 12, 25), datetime(2022, 12, 26)))
             .group_by(Order)
             .having(func.count(Order.id) >= 2))
        print(len(session.execute(q).all()))

        # 5
        q = (select(Customer, func.min(Order.timestamp), func.max(Order.timestamp))
             .join(Customer.orders)
             .group_by(Customer))
        print(len(session.execute(q).all()))

        # 6
        sale_amount = func.sum(OrderItem.quantity * OrderItem.unit_price).label(None)
        q = (select(Manufacturer, sale_amount)
             .join(Manufacturer.products)
             .join(Product.order_items)
             .group_by(Manufacturer)
             .order_by(sale_amount.desc())
             .limit(5))
        print(session.execute(q).all())

        # 7
        count = func.count(ProductReview.product_id).label(None)
        q = (select(Product, func.avg(ProductReview.rating), count)
             .join(Product.reviews)
             .group_by(Product)
             .order_by(count.desc()))
        print(len(session.execute(q).all()))

        # 8
        q = (select(Product, func.avg(ProductReview.rating))
             .join(Product.reviews)
             .where(ProductReview.comment != None)
             .group_by(Product))
        print(len(session.execute(q).all()))

        # 9
        year = func.extract('year', ProductReview.timestamp).label(None)
        month = func.extract('month', ProductReview.timestamp).label(None)
        q = (select(month, func.avg(ProductReview.rating))
             .join(Product.reviews)
             .where(Product.name == 'Commodore 64', year == 2022)
             .group_by(year, month))
        print(session.execute(q).all())

        # 10
        q = (select(Customer, func.min(ProductReview.rating), func.max(ProductReview.rating))
             .join(Customer.product_reviews)
             .group_by(Customer)
             .order_by(Customer.name))
        print(len(session.execute(q).all()))

        # 11
        avg_rating = func.avg(ProductReview.rating).label(None)
        q = (select(Manufacturer, avg_rating)
             .join(Manufacturer.products)
             .join(Product.reviews)
             .group_by(Manufacturer)
             .order_by(avg_rating.desc()))
        print(len(session.execute(q).all()))

        # 12
        avg_rating = func.avg(ProductReview.rating).label(None)
        q = (select(Country, avg_rating)
             .join(Country.products)
             .join(Product.reviews)
             .group_by(Country)
             .order_by(avg_rating.desc()))
        print(len(session.execute(q).all()))