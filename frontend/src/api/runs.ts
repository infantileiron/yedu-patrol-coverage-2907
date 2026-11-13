import {request} from './client';import type {Run} from '../types/entities';
export const list=()=>request<Run[]>('/runs');export const create=(data:unknown)=>request<Run>('/runs','POST',data);
export const cancel=(id:string)=>request<Run>('/runs/'+id+'/cancel','POST');
