from sqlalchemy import func, or_, select

from db import Session
from models import Manufacturer, Product, Country

with Session() as session:
    with session.begin():
        # 1
        q = select(Product).join(Product.countries).where(or_(Country.name == 'USA', Country.name == 'UK')).distinct()
        print(session.scalars(q).all())

        # 2
        q = select(Product).join(Product.countries).where(Country.name != 'USA', Country.name != 'UK').distinct()
        print(len(session.scalars(q).all()))

        # 3
        q = select(Country).join(Country.products).where(Product.cpu.like('%Z80%')).distinct()
        print(session.scalars(q).all())

        # 4
        q = (select(Country)
             .join(Country.products)
             .where(Product.year.between(1970, 1979))
             .order_by(Country.name)
             .distinct())
        print(session.scalars(q).all())

        # 5 (вот тут была интересная проблема с distinct. попробуй добавить - увидишь)
        q = (select(Country)
             .join(Country.products)
             .group_by(Country)
             .order_by(func.count(Product.id).desc(), Country.name)
             .limit(5))
        print(session.scalars(q).all())

        # 6
        q = (select(Manufacturer, Country, func.count(Product.id))
             .join(Manufacturer.products)
             .join(Product.countries)
             .where(or_(Country.name == 'USA', Country.name == 'UK'))
             .group_by(Manufacturer, Country)
             .having(func.count(Product.id.distinct()) > 3)
             )
        print(session.execute(q).all())

        # 7
        q = (select(Manufacturer)
             .join(Manufacturer.products)
             .join(Product.countries)
             .group_by(Manufacturer)
             .having(func.count(Country.id.distinct()) > 1)
             )
        print(session.scalars(q).all())

        # 8
        q = (select(Product)
             .join(Product.countries)
             .join(Product.manufacturer)
             .where(or_(Country.name == 'USA', Country.name == 'UK'))
             .group_by(Product)
             .having(func.count(Country.id.distinct()) > 1)
             )
        print(session.scalars(q).all())
