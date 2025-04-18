from sqlalchemy import func, or_, select

from db import Session
from models import Product

with (Session() as session):
    with session.begin():
        # 1
        q = select(Product).where(Product.year == 1983).order_by(Product.name).limit(3)
        print(session.scalars(q).all())
        # 2
        q = select(Product).where(Product.cpu.like('%Z80%'))
        print(session.scalars(q).all())
        # 3
        q = (select(Product)
             .where(or_(Product.cpu.like('%Z80%'), Product.cpu.like('%6502%')))
             .where(Product.year < 1990)
             .order_by(Product.name))
        print(session.scalars(q).all())
        # 4
        q = select(Product.manufacturer.distinct()).where(Product.year.between(1980, 1989))
        print(session.scalars(q).all())
        # 5
        q = (select(Product.manufacturer.distinct())
             .where(Product.manufacturer.like('T%'))
             .order_by(Product.manufacturer))
        print(session.scalars(q).all())
        # 6
        q = (select(func.min(Product.year), func.max(Product.year), func.count())
             .where(Product.country == 'Croatia'))
        print(session.execute(q).all())
        # 7
        q = select(Product.year, func.count()).group_by(Product.year).order_by(func.count().desc())
        print(session.execute(q).all())
        # 8
        q = (select(func.count(Product.manufacturer.distinct()))
             .where(Product.country == 'USA'))
        print(session.scalar(q))
