from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from models import Student, Grade, Subject, Teacher, Group
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://postgres:dbpassword@localhost:5432/postgres")

def select_1(session: Session):
  return session.query(
    Student.name, func.avg(Grade.grade).label('average_grade')
  ).join(Grade).group_by(Student.id).order_by(func.avg(Grade.grade).desc()).limit(5).all()

def select_2(session: Session, subject_id):
  return session.query(
    Student.name, func.avg(Grade.grade).label('average_grade')
  ).join(Grade).filter(Grade.subject_id == subject_id).group_by(Student.id).order_by(func.avg(Grade.grade).desc()).first()

def select_3(session: Session, subject_id):
  return session.query(
    Group.name, func.avg(Grade.grade).label('average_grade')
  ).select_from(Group).join(Student, Group.id == Student.group_id).join(Grade, Student.id == Grade.student_id).filter(Grade.subject_id == subject_id).group_by(Group.id).all()

def select_4(session: Session):
  return session.query(func.avg(Grade.grade)).scalar()

def select_5(session: Session, teacher_id):
  return session.query(Subject.name).filter(Subject.teacher_id == teacher_id).all()

def select_6(session: Session, group_name):
  return session.query(Student.name).join(Group).filter(Group.name == group_name).all()

def select_7(session: Session, group_name, subject_id):
  return session.query(
    Student.name, Grade.grade, Grade.date_received
  ).join(Group, Student.group_id == Group.id).join(Grade, Student.id == Grade.student_id).filter(
    Group.name == group_name,
    Grade.subject_id == subject_id
  ).all()

def select_8(session: Session, teacher_id):
  return session.query(
    func.avg(Grade.grade).label('average_grade')
  ).join(Subject, Grade.subject_id == Subject.id).filter(
    Subject.teacher_id == teacher_id
  ).scalar()

def select_9(session: Session, student_id):
  return session.query(
    Subject.name
  ).join(Grade, Subject.id == Grade.subject_id).filter(
    Grade.student_id == student_id
  ).distinct().all()

def select_10(session: Session, student_id, teacher_id):
  return session.query(
    Subject.name
  ).join(Grade, Subject.id == Grade.subject_id).filter(
    Grade.student_id == student_id,
    Subject.teacher_id == teacher_id
  ).distinct().all()

if __name__ == "__main__":
  with Session(engine) as session:
    print(select_1(session))
    print(select_2(session, 1))
    print(select_3(session, 1))
    print(select_4(session))
    print(select_5(session, 1))
    print(select_6(session, "Group 1"))
    print(select_7(session, "Group 1", 1))
    print(select_8(session, 1))
    print(select_9(session, 1))
    print(select_10(session, 1, 1))
