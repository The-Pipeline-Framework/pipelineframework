import "./globals.css";

export const metadata = {
  title: "TPFGo Checkout Service Map",
  description: "Service-map view for the TPFGo checkout checkpoint example."
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
