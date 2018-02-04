namespace java com.devansh.humanthrift.generated

exception SystemException {
  1: i32 errorCode,
  2: string message,
}

struct User {
  1: string id,
  2: string email,
  3: string name,
  4: i64 validatedAt
}

struct LoginResult {
  1: string authToken,
  2: User currentUser
}

service AuthenticationService {
  LoginResult login(1: string email, 2: string password) throws(1: SystemException err)
}