from sqlalchemy import Column, JSON, cast, String
from sqlmodel import SQLModel, Field, create_engine, Session

from sqlmodel_repo import SQLModelRepo


class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    username: str
    email: str
    extra_metadata: dict = Field(sa_column=Column(JSON))


# Setup in-memory SQLite engine and metadata
engine = create_engine("sqlite:///:memory:", echo=True)
SQLModel.metadata.create_all(engine)

# Instantiate the repository with the User model and engine
users_repo = SQLModelRepo(model=User, db_engine=engine)


def test_all():
    # Create a new user
    user1 = users_repo.create(username="john_doe", email="john@example.com")

    # Get user by ID
    fetched_user = users_repo.get_by_id(user1.id)

    # Ensure a user that doesn't exist returns None
    assert users_repo.get_by_id(123) is None

    # Get all users
    all_users = users_repo.all()
    assert 1 == len(all_users)

    # Update fetched_user and save changes
    fetched_user.email = "new_email@example.com"
    users_repo.save(fetched_user)

    # Create another user
    users_repo.create(username="joe", email="joe@example.com")

    # Filter users by username
    users = users_repo.filter(username="joe").all()
    assert len(users) == 1
    assert users[0].username == "joe"

    # Filter users where username starts with 'jo'
    users = users_repo.filter(User.username.startswith('jo')).all()
    assert len(users) == 2

    # Delete a user
    users_repo.delete(fetched_user)

    assert len(users_repo.all()) == 1

    # Create a new user with metadata
    users_repo.create(
        username="bob",
        email="bob@example.com",
        extra_metadata={'some_num': 99}
    )

    # Filter users by casting metadata
    users = users_repo.filter(
        cast(User.extra_metadata['some_num'], String) == '99'
    ).all()
    assert users, "cannot find by metadata"

    # Create 10 more users
    for i in range(10):
        users_repo.create(
            username=f'user{i}',
            email=f'user{i}@example.com',
            extra_metadata={'i': i}
        )

    # Verify the total number of users
    assert len(users_repo.all()) == 12

    # Paginate the results (order by username in descending order)
    users, total_count = (
        users_repo.filter()
        .paginate_with_total(0, 4, order_by='username', desc=True)
    )
    assert len(users) == 4
    assert total_count == 12
    assert users[0].username == 'user9'

    # Paginate the results (order by username in ascending order)
    users, total_count = (
        users_repo.filter()
        .paginate_with_total(0, 4, order_by='username', desc=False)
    )
    assert users[0].username == 'bob'

    with Session(engine) as session:
        assert users_repo(session).all()


if __name__ == '__main__':
    test_all()
