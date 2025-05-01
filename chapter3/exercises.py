from sqlalchemy import func, or_, select

from db import Session
from models import Manufacturer, Product

with Session() as session:
    with session.begin():
        # 1
        q = (select(Product)
             .join(Product.manufacturer)
             .where(or_(Manufacturer.name == 'Texas Instruments', Manufacturer.name == 'IBM')))
        print(session.scalars(q).all())

        # 2
        q = select(Manufacturer).join(Manufacturer.products).where(Product.country == 'Brazil').distinct()
        print(session.scalars(q).all())

        # 3
        q = (select(Product)
             .join(Product.manufacturer)
             .where(Manufacturer.name.like('%Research%')))
        print(session.scalars(q).all())

        # 4
        q = (select(Manufacturer)
             .join(Manufacturer.products)
             .where(Product.cpu.ilike('%Z80%'))
             .distinct())
        print(session.scalars(q).all())

        # 5
        q = (select(Manufacturer)
             .join(Manufacturer.products)
             .where(Product.cpu.notilike('%6502%'))
             .distinct())
        print(session.scalars(q).all())

        # 6 (Manufacturers and the year they went to market with their first product, sorted by the year)
        first_year = func.min(Product.year).label(None)
        q = (select(Manufacturer, first_year)
             .join(Manufacturer.products)
             .group_by(Manufacturer)
             .order_by(first_year))
        print(session.execute(q).all())

        # 7
        q = (select(Manufacturer)
             .join(Manufacturer.products)
             .group_by(Manufacturer)
             .having(func.count(Product.id).between(3, 5)))
        print(session.execute(q).all())

        # 8
        q = (select(Manufacturer)
             .join(Manufacturer.products)
             .group_by(Manufacturer)
             .having(func.max(Product.year) - func.min(Product.year) > 5))
        print(session.execute(q).all())
