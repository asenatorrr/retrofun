from datetime import datetime

from sqlalchemy import func, or_, select
from sqlalchemy.orm import aliased

from db import Session
from models import BlogArticle, BlogView, Language

with Session() as session:
    with session.begin():
        # 1
        year, month = func.extract('year', BlogView.timestamp), func.extract('month', BlogView.timestamp)
        q = (select(BlogArticle)
             .where(year == 2020, month == 3)
             .join(BlogArticle.views)
             .group_by(BlogArticle)
             .having(func.count(BlogView.id) > 50))

        print(session.scalars(q).all())

        # 2
        OriginalBlogArticle = aliased(BlogArticle)
        q = (select(OriginalBlogArticle)
             .join(OriginalBlogArticle.translations)
             .group_by(OriginalBlogArticle)
             .order_by(func.count(BlogArticle.id).desc(), OriginalBlogArticle.title)
             .limit(1))

        print(session.scalar(q))

        # 3
        year, month = func.extract('year', BlogView.timestamp), func.extract('month', BlogView.timestamp)
        q = (select(Language, func.count(BlogView.id))
             .where(year == 2022, month == 3)
             .join(BlogView.article)
             .join(BlogArticle.language)
             .group_by(Language))

        print(session.execute(q).all())

        # 4
        q = (select(BlogArticle, func.count(BlogView.id))
             .join(BlogView.article)
             .join(BlogArticle.language)
             .where(Language.name == 'German')
             .group_by(BlogArticle))

        print(len(session.execute(q).all()))

        # 4
        year, month = func.extract('year', BlogView.timestamp), func.extract('month', BlogView.timestamp)
        q = (select(month, func.count(BlogView.id))
             .where(year == 2022)
             .group_by(month)
             .order_by(month))

        print(session.execute(q).all())

        # 5
        year, month, day = (func.extract('year', BlogView.timestamp),
                            func.extract('month', BlogView.timestamp),
                            func.extract('day', BlogView.timestamp))
        q = (select(day, func.count(BlogView.id))
             .where(year == 2022, month == 2)
             .group_by(day)
             .order_by(day))

        print(session.execute(q).all())
