from contextlib import contextmanager
from typing import Optional

from sqlmodel import Session, SQLModel, select, func, text


@contextmanager
def reuse_session_or_new(db_engine=None, session: Optional[Session] = None):
    """
    Context manager to wrap session reuse or creation logic.

    :param session: An existing session to reuse. If None,
        a new session is created.
    :param db_engine: The database engine to use if creating a new session.
    """
    should_close = False
    try:
        # If session is None, create a new session using the provided db_engine
        if session is None:
            if db_engine is None:
                raise ValueError(
                    "No session and no db_engine provided to create a session."
                )
            session = Session(db_engine)
            should_close = True

        # Yield the session for use in the context block
        yield session
    finally:
        # Close the session if it was created inside this context manager
        if should_close:
            session.close()


class CollectionResult:
    def __init__(self, stmt, model, db_engine, session=None):
        self.stmt = stmt
        self.model = model
        self.db_engine = db_engine
        self.session = session

    def paginate(
        self,
        offset: int,
        limit: int,
        order_by: str,
        desc: bool = False
    ) -> list:
        """Paginate results"""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            return self._paginate(session, offset, limit, order_by, desc)

    def paginate_with_total(
        self,
        offset: int,
        limit: int,
        order_by: str,
        desc: bool = False
    ) -> (list, int):
        """Paginate results and fetch total count

        Returns:
            tuple(list, int) - Items and total count.
        """
        with reuse_session_or_new(self.db_engine, self.session) as session:
            count_stmt = select(func.count()).select_from(self.stmt.subquery())
            count = session.execute(count_stmt).scalar()
            results = self._paginate(session, offset, limit, order_by, desc)
            return results, count

    def _paginate(
        self,
        session,
        offset: int,
        limit: int,
        order_by: str,
        desc: bool = False
    ) -> list:
        order_by = getattr(self.model, order_by)
        if desc:
            order_by = getattr(order_by, 'desc')()
        return session.exec(
            self.stmt.order_by(order_by).offset(offset).limit(limit)
        ).all()

    def all(self) -> list:
        """Get all results"""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            return session.exec(self.stmt).all()

    def count(self):
        """Get total results count"""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            count_stmt = select(func.count()).select_from(self.stmt.subquery())
            return session.execute(count_stmt).scalar()


class SQLModelRepo:
    def __init__(self, model: SQLModel, db_engine):
        """ Generic repository for SQLModel.

        Args:
            model (SQLModel): The SQLModel class (table) for which
                the repo is instantiated.
            db_engine: The SQLAlchemy engine linked to the database.

        Usage:
            users_repo = SQLModelRepo(model=User, db_engine=engine)
            users_repo.get_by_id(1)
        """
        self.model = model
        self.db_engine = db_engine
        self._session = None

    def __call__(self, session):
        new_repo = SQLModelRepo(model=self.model, db_engine=self.db_engine)
        new_repo._session = session
        return new_repo

    def create(self, **kwargs):
        """Create a new record and save to the database."""
        instance = self.model(**kwargs)
        with reuse_session_or_new(self.db_engine, self._session) as session:
            session.add(instance)
            session.commit()
            session.refresh(instance)
        return instance

    def get_by_id(self, id, *fields):
        """Fetch an object by its primary key."""
        select_obj = self._get_select_obj(fields)
        with reuse_session_or_new(self.db_engine, self._session) as session:
            return session.exec(
                select(*select_obj).where(
                    getattr(self.model, 'id') == id
                )
            ).first()

    def save(self, instance):
        """Save the current object (instance) to the database."""
        with reuse_session_or_new(self.db_engine, self._session) as session:
            session.add(instance)
            session.commit()
            session.refresh(instance)

    def update(self, id, **kwargs):
        """Record partial update."""
        with reuse_session_or_new(self.db_engine, self._session) as session:
            set_statements = ", ".join(
                f"{field} = :{field}" for field in kwargs.keys()
            )
            kwargs['id'] = id
            query = f"""
                    UPDATE {self.model.__tablename__}
                    SET {set_statements}
                    WHERE id = :id
                """
            session.execute(text(query), kwargs)
            session.commit()

    def delete(self, instance):
        """Delete an object from the database."""
        with reuse_session_or_new(self.db_engine, self._session) as session:
            session.delete(instance)
            session.commit()

    def all(self, *fields) -> list:
        """Return all records."""
        select_obj = self._get_select_obj(fields)
        with reuse_session_or_new(self.db_engine, self._session) as session:
            return session.exec(select(*select_obj)).all()

    def filter(self, *filters, _fields=(), **kwargs) -> CollectionResult:
        """Filter records based on provided conditions."""
        select_obj = self._get_select_obj(_fields)
        stmt = select(*select_obj).where(
            *filters,
            *[
                getattr(self.model, k) == v
                if isinstance(k, str) else k == v
                for k, v in kwargs.items()
            ]
        )
        return CollectionResult(
            stmt=stmt,
            model=self.model,
            db_engine=self.db_engine,
            session=self._session
        )

    def _get_select_obj(self, fields=None):
        return (
            [self.model] if not fields
            else [getattr(self.model, f) for f in fields]
        )
