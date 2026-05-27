import "./globals.css";

export const metadata = {
  title: "Restaurant Approval",
  description: "Human approval inbox for the TPF interaction-api await example."
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
