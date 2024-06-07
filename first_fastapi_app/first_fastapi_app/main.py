from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from first_fastapi_app import settings
from sqlmodel import SQLModel, create_engine, Session, Field, select
from contextlib import asynccontextmanager
from typing import Optional
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio, json
from starlette.datastructures import Secret

class Todo(SQLModel, table=True ):
    id: Optional[int] = Field(default=None, primary_key=True)
    content:str = Field(index=True)

connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)

engine = create_engine(connection_string, connect_args={}, pool_recycle=300) 
def create_db_tables():
    SQLModel.metadata.create_all(engine)


async def consume():
    consumer = AIOKafkaConsumer(
        'api-topic-1',
        bootstrap_servers='localhost:9092',  # Pointing to local broker
        group_id='api-consumer-1',
        auto_offset_reset='earliest'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print("Consumed: ", msg.value.decode())
    finally:
        await consumer.stop()


# async def consume_messages(topic_name, bootstrap_server, consumer_group_id):
#     consumer = AIOKafkaConsumer(topic_name, bootstrap_servers=bootstrap_server, group_id=consumer_group_id)
#     await consumer.start()
#     try:
#         async for message in consumer:
#             print(f"Received message: {message.value.decode('utf-8')}")
#             # Process the message here
#             # For example, you can insert the message into the database
#             todo_dict = json.loads(message.value.decode('utf-8'))
#             todo = Todo(**todo_dict)
#             async with get_session() as session:
#                 session.add(todo)
#                 session.commit()
#                 session.refresh(todo)
#                 print(f"Inserted todo: {todo}")
#     finally:
#         await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Creating Database Tables ...")
    task = asyncio.create_task(consume('api-topic-1', 'localhost:9092', 'api-consumer-1'))
    create_db_tables()
    try:
        yield
    finally:
        task.cancel()
        await task  # Wait for the task to start before proceeding


app = FastAPI(
    lifespan=lifespan,
    title="Fastapi Endpoints & Neon DB",
    version= "1.0.0",
    servers=[{
        "url" : "http://127.0.0.1:8000",
        "description" : " Development Server"
    }]
)

##### SWAGGER CONFIG FOR "CORS", "FETCH TO FAIL ISSUES"
origins = [
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "https://localhost:8000",
    "https://127.0.0.1:8000",
    "http://localhost:8080",  
    "http://127.0.0.1:8080", 
    "https://localhost:8080",  
    "https://127.0.0.1:8080",  


    # Add any other origins if needed
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
def get_session(): ### Read Note:4
    with Session(engine) as session:
        yield session

@app.get("/")
def read_root():
    message = {"Hello":"My First API Endpoint"}
    return message

@app.post("/todo/")
def create_todo(todo:Todo, session: Session= Depends(get_session)):

    session.add(todo)
    session.commit()
    session.refresh(todo)
    print(f"Capture Created Todo : {todo}")
    return todo

@app.get("/todo/")
def read_todo_list(session: Session= Depends(get_session)):
    todos = session.exec(select(Todo)).all()
    return todos

@app.delete("/todo/{todo_id}")
def delete_todo(todo_id:int, session: Session= Depends(get_session)):
  db_todo = session.get(Todo, todo_id)
  if db_todo:
      session.delete(db_todo)
      session.commit()
      return {"message":"Todo deleted successfully"}
  else:
      raise HTTPException(status_code= 404, detail="Todo not found")

## UPDATE_TODO_ENDPOINT
class UpdateTodoRequest(BaseModel):
    content:str

@app.put("/todo/{todo_id}")
def update_todo(todo_id:int,todo_update:UpdateTodoRequest, session: Session= Depends(get_session)): 
    todo = session.get(Todo, todo_id) #fetch existing todo by id
    if todo: # if true (not empty)
        todo.content = todo_update.content #update the todo content
        session.commit()
        print(f"Todo:{todo} Updated to:{todo_update.content}")
        return {"message":"Todo successfully updated"}
    else:
        raise HTTPException(status_code=404, detail="Todo not Found")
    
    # Kafka Producer as a dependency
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

@app.post("/produce/", response_model=Todo)
async def create_todo(todo: Todo, session: Session= Depends(get_session), 
                      producer: AIOKafkaProducer= Depends(get_kafka_producer),
                      topic_name: str= settings.KAFKA_TOPIC)->Todo:
        todo_dict = {field: getattr(todo, field) for field in todo.dict()}
        todo_json = json.dumps(todo_dict).encode("utf-8")
        print("todoJSON:", todo_json)
        # Produce message
        await producer.send_and_wait(topic_name, todo_json)
        session.add(todo)
        session.commit()
        session.refresh(todo)
        return todo


