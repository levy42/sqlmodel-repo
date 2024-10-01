from contextlib import contextmanager
from typing import Optional

from sqlmodel import Session, SQLModel, select, delete, update, func

try:
    from fastapi.exceptions import HTTPException
except Exception:
    pass


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


class SQLModelRepo:
    def __init__(
        self, model: SQLModel, db_engine,
        init_stmt=None, session=None
    ):
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
        self._init_stmt = init_stmt
        self.db_engine = db_engine
        self.session = session

    def __call__(self, session):
        new_repo = SQLModelRepo(model=self.model, db_engine=self.db_engine)
        new_repo.session = session
        return new_repo

    def create(self, **kwargs):
        """Create a new record and save to the database."""
        instance = self.model(**kwargs)
        with reuse_session_or_new(self.db_engine, self.session) as session:
            session.add(instance)
            session.commit()
            session.refresh(instance)
        return instance

    def get_by_id(self, id, *fields):
        """Fetch an object by its primary key."""
        stmt = self.init_stmt(*fields)
        with reuse_session_or_new(self.db_engine, self.session) as session:
            return session.exec(
                stmt.where(
                    getattr(self.model, 'id') == id
                )
            ).first()

    def save(self, instance):
        """Save the current object (instance) to the database."""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            session.add(instance)
            session.commit()
            session.refresh(instance)

    def save_or_update(self, instance):
        """Save the current object (instance) to the database."""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            existing_obj = session.exec(
                select(self.model).where(
                    self.model.id == instance.id
                )
            ).first()
            if existing_obj:
                for k, v in instance.model_dump().items():
                    setattr(existing_obj, k, v)
                    session.add(existing_obj)
                    instance = existing_obj
            else:
                session.add(instance)
            session.commit()
            session.refresh(instance)

    def update(self, id, **kwargs):
        """Record partial update."""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            update_stmt = update(self.model).where(
                self.model.id == id
            ).values(**kwargs)
            session.execute(update_stmt)
            session.commit()

    def update_all(self, **kwargs):
        """Partial update for all selected records."""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            if self._init_stmt:
                update_stmt = update(self.model).where(
                    self.init_stmt().whereclause
                ).values(**kwargs)
            else:
                update_stmt = update(self.model).values(**kwargs)
            session.execute(update_stmt)
            session.commit()

    def delete(self, instance):
        """Delete an object from the database."""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            session.delete(instance)
            session.commit()

    def delete_all(self):
        """Delete all records in query."""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            if self._init_stmt:
                delete_stmt = delete(self.model).where(
                    self.init_stmt().whereclause
                )
            else:
                delete_stmt = delete(self.model)
            session.execute(delete_stmt)
            session.commit()

    def filter(self, *filters, _fields=(), **kwargs) -> 'SQLModelRepo':
        """Filter records based on provided conditions."""
        stmt = self.init_stmt(*_fields).where(
            *filters,
            *[
                getattr(self.model, k) == v
                if isinstance(k, str) else k == v
                for k, v in kwargs.items()
            ]
        )
        return SQLModelRepo(
            init_stmt=stmt,
            model=self.model,
            db_engine=self.db_engine,
            session=self.session
        )

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
            count_stmt = select(func.count()).select_from(
                self.init_stmt().subquery()
            )
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
            self.init_stmt().order_by(order_by).offset(offset).limit(limit)
        ).all()

    def all(self) -> list:
        """Get all results"""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            return session.exec(self.init_stmt()).all()

    def count(self):
        """Get total results count"""
        with reuse_session_or_new(self.db_engine, self.session) as session:
            count_stmt = select(func.count()).select_from(
                self.init_stmt().subquery()
            )
            return session.execute(count_stmt).scalar()

    def first(self):
        with reuse_session_or_new(self.db_engine, self.session) as session:
            return session.exec(self.init_stmt()).first()

    def get_or_404(self, id):
        if not (obj := self.get_by_id(id)):
            raise HTTPException(
                status_code=404,
                detail=f'{self.model.__name__.title()} with id {id} not found'
            )
        return obj

    def delete_or_404(self, id):
        obj = self.get_or_404(id)
        self.delete(obj)

    def update_or_404(self, id, **kwargs):
        if self.get_or_404(id):
            self.update(id, **kwargs)

    def _get_select_obj(self, fields=None):
        return (
            [self.model] if not fields
            else [getattr(self.model, f) for f in fields]
        )

    def init_stmt(self, *fields):
        if self._init_stmt is not None:
            return self._init_stmt
        else:
            return select(*self._get_select_obj(fields))
