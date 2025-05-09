# Example Dockerfile for Frontend (React)
FROM node:18-alpine as build

WORKDIR /app
COPY package.json package-lock.json ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
