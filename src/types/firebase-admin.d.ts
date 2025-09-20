import { FirebaseApp } from "firebase-admin/app";

declare global {
  var firebaseAdminApp: FirebaseApp | undefined;
}
